import hashlib
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine
from pipeline.logger import get_logger

log = get_logger("bill_summarizer")

_NLP = None


def _get_nlp():
    global _NLP
    if _NLP is None:
        import spacy
        _NLP = spacy.load("en_core_web_sm")
    return _NLP


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _score_sentence(sent) -> float:
    n_ents = len(sent.ents)
    n_chunks = len(list(sent.noun_chunks))
    length_bonus = min(len(sent), 40) / 40
    return n_ents * 3 + n_chunks + length_bonus


def _clean(s: str) -> str:
    import re
    return re.sub(r"\s+", " ", s).strip()


def _find_bill_body_start(text: str) -> int:
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
    return min(1000, len(text) // 4)


def extractive_summary(text: str,
                       n_short: int = 2,
                       n_detailed: int = 5,
                       max_short_words: int = 60,
                       max_detailed_words: int = 220) -> tuple[str, str]:
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
        alpha_words = [w for w in words if w.isalpha()]
        if alpha_words and sum(1 for w in alpha_words if w.isupper()) / len(alpha_words) > 0.55:
            return True
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
    input_sha = _sha256(bill_text)
    engine = get_engine()

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
