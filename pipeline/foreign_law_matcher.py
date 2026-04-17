"""
Foreign law comparison — keyword-overlap similarity (free, no API).

For each bill we compare its YAKE keywords against the title + summary text
of every foreign law in the database using TF-IDF cosine similarity computed
locally via scikit-learn.

Returns the top-3 foreign laws with similarity_score in [0, 100] and a
plain-English explanation built from the overlap terms.

Cache key: bill's text_sha256. Re-runs only when text changes.
"""
import math
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine
from pipeline.logger import get_logger

log = get_logger("foreign_law_matcher")


def _tokenise(s: str) -> list[str]:
    """Lowercase alpha-only tokens, length ≥ 3, excluding stopwords."""
    _STOP = {
        "the", "and", "for", "that", "this", "with", "from", "are", "has",
        "have", "been", "will", "shall", "may", "any", "all", "its", "their",
        "act", "law", "bill", "kenya", "section", "subsection", "provision",
    }
    tokens = []
    for tok in s.lower().split():
        word = "".join(c for c in tok if c.isalpha())
        if len(word) >= 3 and word not in _STOP:
            tokens.append(word)
    return tokens


def _tf(tokens: list[str]) -> dict[str, float]:
    if not tokens:
        return {}
    freq: dict[str, int] = {}
    for t in tokens:
        freq[t] = freq.get(t, 0) + 1
    total = len(tokens)
    return {w: c / total for w, c in freq.items()}


def _cosine(vec_a: dict[str, float], vec_b: dict[str, float]) -> float:
    shared = set(vec_a) & set(vec_b)
    if not shared:
        return 0.0
    dot = sum(vec_a[w] * vec_b[w] for w in shared)
    mag_a = math.sqrt(sum(v * v for v in vec_a.values()))
    mag_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def _overlap_terms(vec_a: dict[str, float],
                   vec_b: dict[str, float],
                   top_n: int = 6) -> list[str]:
    """Return the highest-weighted shared terms."""
    shared = {w: (vec_a[w] + vec_b[w]) / 2
              for w in set(vec_a) & set(vec_b)}
    return sorted(shared, key=lambda w: shared[w], reverse=True)[:top_n]


def _build_explanation(bill_title: str, law_name: str,
                        law_jurisdiction: str, terms: list[str]) -> str:
    if not terms:
        return (
            f"The Kenyan bill and {law_name} ({law_jurisdiction}) share "
            "legislative subject matter based on structural text similarity."
        )
    term_str = ", ".join(f'"{t}"' for t in terms[:4])
    return (
        f'"{bill_title}" shares key concepts with {law_name} ({law_jurisdiction}), '
        f"including {term_str}. "
        f"Both instruments address similar regulatory objectives in overlapping domains."
    )


def match_foreign_laws(bill_id: str, bill_title: str, bill_text: str,
                        bill_keywords: list[str],
                        text_sha256: str) -> Optional[list[dict]]:
    """
    Compare bill against all seeded foreign laws using keyword TF-IDF cosine.
    Returns list of match dicts or None if already cached / no foreign laws.
    """
    engine = get_engine()

    with engine.connect() as conn:
        existing = conn.execute(
            text("""
                SELECT 1 FROM bill_foreign_matches bfm
                JOIN bills b ON b.bill_id = bfm.bill_id
                WHERE bfm.bill_id = :bid AND b.text_sha256 = :sha
                LIMIT 1
            """),
            {"bid": bill_id, "sha": text_sha256},
        ).fetchone()
        if existing:
            log.info(f"  {bill_id}: Foreign matches cached, skipping")
            return None

    with engine.connect() as conn:
        foreign_laws = conn.execute(
            text("SELECT id, jurisdiction, law_name, summary FROM foreign_laws ORDER BY id")
        ).fetchall()

    if not foreign_laws:
        log.warning("  No foreign laws seeded — skipping foreign law matching")
        return []

    bill_corpus = f"{bill_title} {' '.join(bill_keywords)} {bill_text[:4000]}"
    bill_vec = _tf(_tokenise(bill_corpus))

    matches = []
    for law_id, jurisdiction, law_name, summary in foreign_laws:
        law_corpus = f"{law_name} {jurisdiction} {summary or ''}"
        law_vec = _tf(_tokenise(law_corpus))
        sim = _cosine(bill_vec, law_vec)
        score = int(round(sim * 100))
        if score >= 20:
            terms = _overlap_terms(bill_vec, law_vec)
            matches.append({
                "foreign_law_id": law_id,
                "similarity_score": score,
                "explanation": _build_explanation(bill_title, law_name,
                                                  jurisdiction, terms),
            })

    matches.sort(key=lambda m: m["similarity_score"], reverse=True)
    matches = matches[:3]

    if not matches:
        log.info(f"  {bill_id}: No foreign law matches above threshold")
        return []

    with engine.connect() as conn:
        for match in matches:
            conn.execute(
                text("""
                    INSERT INTO bill_foreign_matches
                        (bill_id, foreign_law_id, similarity_score, explanation)
                    VALUES (:bill_id, :foreign_law_id, :score, :explanation)
                    ON CONFLICT (bill_id, foreign_law_id) DO UPDATE SET
                        similarity_score = EXCLUDED.similarity_score,
                        explanation      = EXCLUDED.explanation,
                        generated_at     = NOW()
                """),
                {
                    "bill_id": bill_id,
                    "foreign_law_id": match["foreign_law_id"],
                    "score": match["similarity_score"],
                    "explanation": match["explanation"],
                },
            )
        conn.commit()

    log.info(f"  {bill_id}: {len(matches)} foreign law matches stored")
    return matches


def _mark_checked(engine, bill_id: str):
    """Stamp foreign_match_checked_at so bills with no matches aren't re-scanned."""
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE bills SET foreign_match_checked_at = NOW() WHERE bill_id = :bid"),
            {"bid": bill_id},
        )
        conn.commit()


def match_all_bills() -> dict:
    """Run foreign law matching for all bills with parsed text."""
    engine = get_engine()
    stats = {"matched": 0, "cached": 0, "failed": 0, "no_laws": 0}

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT b.bill_id, b.title, t.full_text, b.text_sha256,
                       COALESCE(
                           array_agg(bk.keyword ORDER BY bk.relevance_score DESC)
                           FILTER (WHERE bk.keyword IS NOT NULL),
                           '{}'
                       ) AS keywords
                FROM bills b
                JOIN raw_bill_text t ON t.bill_id = b.bill_id
                LEFT JOIN bill_keywords bk ON bk.bill_id = b.bill_id
                WHERE t.parse_status = 'success'
                  AND t.full_text IS NOT NULL
                  AND b.text_sha256 IS NOT NULL
                  AND b.foreign_match_checked_at IS NULL
                GROUP BY b.bill_id, b.title, t.full_text, b.text_sha256
            """)
        ).fetchall()

    log.info(f"Bills pending foreign law matching: {len(rows)}")

    for bill_id, title, full_text, text_sha256, keywords in rows:
        try:
            result = match_foreign_laws(
                bill_id, title, full_text, list(keywords or []), text_sha256
            )
            _mark_checked(engine, bill_id)
            if result is None:
                stats["cached"] += 1
            elif result:
                stats["matched"] += 1
            else:
                stats["no_laws"] += 1
        except Exception as exc:
            log.error(f"  {bill_id}: Foreign law matching failed — {exc}")
            stats["failed"] += 1

    return stats
