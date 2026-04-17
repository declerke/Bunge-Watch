"""
Extractive bill summariser — 100% free, no API required.

Uses spaCy's sentence segmentation and a simple named-entity + noun-chunk
scoring heuristic to select the most informative sentences from a bill.

SHORT_SUMMARY  : top 2 ranked sentences  (≤ ~50 words)
DETAILED_SUMMARY: top 5 ranked sentences  (≤ ~200 words)

The same bill_summaries table and XCom interface are preserved so the
rest of the pipeline (dbt, Streamlit) needs no changes.
"""
import hashlib
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine
from pipeline.logger import get_logger

log = get_logger("bill_summarizer")

_NLP = None  # lazy-loaded spaCy model


def _get_nlp():
    global _NLP
    if _NLP is None:
        import spacy
        _NLP = spacy.load("en_core_web_sm")
    return _NLP


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _score_sentence(sent) -> float:
    """
    Score a spaCy Span by informativeness proxy:
    named entities × 3 + noun chunks × 1.
    Longer sentences that aren't too long get a mild length boost.
    """
    n_ents = len(sent.ents)
    n_chunks = len(list(sent.noun_chunks))
    length_bonus = min(len(sent), 40) / 40  # normalised [0,1]
    return n_ents * 3 + n_chunks + length_bonus


def _clean(s: str) -> str:
    """Collapse whitespace/newlines to single spaces."""
    import re
    return re.sub(r"\s+", " ", s).strip()


def _find_bill_body_start(text: str) -> int:
    """
    Return the character offset where the bill body actually begins.
    Kenyan bills follow a standard structure; skip the cover-page header
    by finding "A Bill for", "AN ACT", "Objects and Reasons", or
    "ENACTED by the Parliament".
    """
    import re
    patterns = [
        r"Objects\s+and\s+Reasons",
        r"A\s+Bill\s+for\s+AN\s+ACT",
        r"AN\s+ACT\s+of\s+Parliament",
        r"ENACTED\s+by\s+the\s+Parliament",
        r"Statement\s+of\s+Objects",
        r"ARRANGEMENT\s+OF\s+CLAUSES",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.start()
    # Fallback: skip the first 1 000 characters (cover page)
    return min(1000, len(text) // 4)


def extractive_summary(text: str,
                        n_short: int = 2,
                        n_detailed: int = 5,
                        max_short_words: int = 60,
                        max_detailed_words: int = 220) -> tuple[str, str]:
    """
    Extract SHORT (≤60 words) and DETAILED (≤220 words) summaries.
    Starts from the bill-body (after the cover page) to avoid OCR headers.
    Scores sentences by named-entity + noun-chunk density.
    """
    import re
    start = _find_bill_body_start(text)
    body = text[start:start + 8000]

    nlp = _get_nlp()
    doc = nlp(body)

    def _is_junk(sent) -> bool:
        raw = sent.text.strip()
        words = raw.split()
        if len(words) < 5:
            return True
        # Reject lines that are mostly uppercase (headers/section titles)
        alpha_words = [w for w in words if w.isalpha()]
        if alpha_words and sum(1 for w in alpha_words if w.isupper()) / len(alpha_words) > 0.55:
            return True
        # Reject if more than half the words look like OCR fragments (≤2 chars)
        if sum(1 for w in words if len(w) <= 2) / len(words) > 0.4:
            return True
        return False

    sents = [(sent, _score_sentence(sent))
             for sent in doc.sents
             if not _is_junk(sent)]

    if not sents:
        fallback = _clean(body[:500])
        return fallback[:250], fallback

    ranked = sorted(sents, key=lambda x: x[1], reverse=True)

    def _build(top_n, max_words):
        candidates = ranked[:max(top_n, 10)]
        candidates.sort(key=lambda x: x[0].start)
        words_used, parts = 0, []
        for sent, _ in candidates:
            cleaned = _clean(sent.text)
            wc = len(cleaned.split())
            if words_used + wc > max_words and parts:
                break
            parts.append(cleaned)
            words_used += wc
            if len(parts) >= top_n:
                break
        return " ".join(parts)

    return _build(n_short, max_short_words), _build(n_detailed, max_detailed_words)


def summarise_bill(bill_id: str, bill_text: str) -> Optional[tuple[str, str]]:
    """
    Generate and store extractive English summary for a bill.
    Returns (short_summary, detailed_summary) or None on failure.
    Cache key: SHA-256 of input text — skips re-summarisation on unchanged text.
    """
    input_sha = _sha256(bill_text)
    engine = get_engine()

    # Cache check
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT summary_short, summary_detailed FROM bill_summaries
                WHERE bill_id = :bid AND language = 'en' AND input_sha256 = :sha
            """),
            {"bid": bill_id, "sha": input_sha},
        ).fetchone()
        if row:
            log.info(f"  {bill_id}: Summary cached, skipping")
            return row[0], row[1]

    try:
        short, detailed = extractive_summary(bill_text)

        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO bill_summaries
                        (bill_id, language, summary_short, summary_detailed,
                         model_version, input_sha256)
                    VALUES (:bid, 'en', :short, :detailed, :model, :sha)
                    ON CONFLICT (bill_id, language) DO UPDATE SET
                        summary_short    = EXCLUDED.summary_short,
                        summary_detailed = EXCLUDED.summary_detailed,
                        model_version    = EXCLUDED.model_version,
                        input_sha256     = EXCLUDED.input_sha256,
                        generated_at     = NOW()
                """),
                {
                    "bid": bill_id,
                    "short": short,
                    "detailed": detailed,
                    "model": "extractive-spacy-v1",
                    "sha": input_sha,
                },
            )
            conn.commit()

        log.info(f"  {bill_id}: Summary generated ({len(short)}/{len(detailed)} chars)")
        return short, detailed

    except Exception as exc:
        log.error(f"  {bill_id}: Summarisation failed — {exc}")
        return None


def summarise_all_bills() -> dict:
    """Summarise all bills with parsed text that don't yet have an English summary."""
    engine = get_engine()
    stats = {"summarised": 0, "cached": 0, "failed": 0}

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT t.bill_id, t.full_text
                FROM raw_bill_text t
                JOIN bills b ON b.bill_id = t.bill_id
                WHERE t.parse_status = 'success'
                  AND t.full_text IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM bill_summaries s
                      WHERE s.bill_id = t.bill_id
                        AND s.language = 'en'
                        AND s.input_sha256 = encode(
                            digest(t.full_text, 'sha256'), 'hex'
                        )
                  )
            """)
        ).fetchall()

    log.info(f"Bills pending summarisation: {len(rows)}")

    for bill_id, full_text in rows:
        result = summarise_bill(bill_id, full_text)
        if result:
            stats["summarised"] += 1
        else:
            stats["failed"] += 1

    return stats
