"""
Keyword extraction using spaCy NER + YAKE.
YAKE is used over KeyBERT to avoid heavy torch dependency.
Top 10 keywords per bill stored in bill_keywords table.
"""
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine
from pipeline.logger import get_logger

log = get_logger("keyword_extractor")

_nlp = None
_yake = None


def _get_nlp():
    global _nlp
    if _nlp is None:
        import spacy
        _nlp = spacy.load("en_core_web_sm")
    return _nlp


def _get_yake():
    global _yake
    if _yake is None:
        import yake
        _yake = yake.KeywordExtractor(
            lan="en",
            n=3,         # up to 3-gram phrases
            dedupLim=0.7,
            top=15,
        )
    return _yake


def extract_keywords(text_content: str) -> list[tuple[str, float]]:
    """
    Return list of (keyword, score) tuples. Score is normalised 0–1,
    where higher = more relevant (YAKE raw scores are inverted).
    """
    if not text_content or len(text_content.strip()) < 100:
        return []

    extractor = _get_yake()
    raw = extractor.extract_keywords(text_content[:8000])  # cap at 8K chars

    if not raw:
        return []

    # YAKE: lower score = more relevant. Normalise to 0-1 where 1 = most relevant.
    max_score = max(s for _, s in raw) or 1.0
    normalised = [(kw, round(1 - (s / max_score), 4)) for kw, s in raw]
    normalised.sort(key=lambda x: x[1], reverse=True)

    return normalised[:10]


def enrich_bill_keywords(bill_id: str, text_content: str) -> int:
    """Extract and store keywords for one bill. Returns count stored."""
    keywords = extract_keywords(text_content)
    if not keywords:
        return 0

    engine = get_engine()
    with engine.connect() as conn:
        for keyword, score in keywords:
            conn.execute(
                text("""
                    INSERT INTO bill_keywords (bill_id, keyword, relevance_score)
                    VALUES (:bill_id, :keyword, :score)
                    ON CONFLICT (bill_id, keyword) DO UPDATE SET
                        relevance_score = EXCLUDED.relevance_score
                """),
                {"bill_id": bill_id, "keyword": keyword.lower(), "score": score},
            )
        conn.commit()

    log.info(f"  {bill_id}: Stored {len(keywords)} keywords")
    return len(keywords)


def enrich_all_bills() -> dict:
    """Enrich keywords for all bills with parsed text but no keywords yet."""
    engine = get_engine()
    stats = {"enriched": 0, "skipped": 0}

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT t.bill_id, t.full_text
                FROM raw_bill_text t
                WHERE t.parse_status = 'success'
                  AND t.full_text IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM bill_keywords k WHERE k.bill_id = t.bill_id
                  )
            """)
        ).fetchall()

    log.info(f"Bills pending keyword extraction: {len(rows)}")

    for bill_id, full_text in rows:
        count = enrich_bill_keywords(bill_id, full_text)
        if count:
            stats["enriched"] += 1
        else:
            stats["skipped"] += 1

    return stats
