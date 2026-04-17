"""Cached database queries for the Streamlit app."""
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('POSTGRES_USER', 'bungewatch')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'bungewatch')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'bungewatch')}"
        )
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


@st.cache_data(ttl=600)
def get_active_bills(stage_filter=None, chamber_filter=None,
                     year_filter=None, keyword_filter=None) -> pd.DataFrame:
    q = """
        SELECT bill_id, title, sponsor, chamber, date_introduced,
               current_stage, bill_year, source_url, pdf_url,
               summary_short, keyword_tags, foreign_match_count,
               last_updated_at
        FROM public_marts.mart_active_bills
        WHERE 1=1
    """
    params = {}
    if stage_filter:
        q += " AND current_stage = :stage"
        params["stage"] = stage_filter
    if chamber_filter:
        q += " AND chamber = :chamber"
        params["chamber"] = chamber_filter
    if year_filter:
        q += " AND bill_year = :year"
        params["year"] = year_filter
    if keyword_filter:
        q += " AND LOWER(keyword_tags) LIKE :kw"
        params["kw"] = f"%{keyword_filter.lower()}%"
    q += " ORDER BY last_updated_at DESC"

    with get_engine().connect() as conn:
        return pd.read_sql(text(q), conn, params=params)


@st.cache_data(ttl=600)
def get_bill_detail(bill_id: str) -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        bill = conn.execute(
            text("""
                SELECT b.bill_id, b.title, b.bill_number, b.sponsor, b.sponsor_party,
                       b.chamber, b.date_introduced, b.gazette_no, b.current_stage,
                       b.is_passed, b.assent_date, b.source_url, b.pdf_url,
                       b.first_seen_at, b.last_updated_at,
                       s.summary_short, s.summary_detailed, s.generated_at
                FROM bills b
                LEFT JOIN bill_summaries s ON s.bill_id = b.bill_id AND s.language = 'en'
                WHERE b.bill_id = :bid
            """),
            {"bid": bill_id},
        ).fetchone()

        stages = conn.execute(
            text("""
                SELECT stage_name, stage_order, stage_date, observed_at
                FROM public_intermediate.int_bill_stages
                WHERE bill_id = :bid
                ORDER BY stage_order
            """),
            {"bid": bill_id},
        ).fetchall()

        keywords = conn.execute(
            text("""
                SELECT keyword, relevance_score
                FROM bill_keywords
                WHERE bill_id = :bid
                ORDER BY relevance_score DESC
                LIMIT 10
            """),
            {"bid": bill_id},
        ).fetchall()

        foreign_matches = conn.execute(
            text("""
                SELECT fl.jurisdiction, fl.law_name, fl.law_year, fl.full_text_url,
                       bfm.similarity_score, bfm.explanation
                FROM bill_foreign_matches bfm
                JOIN foreign_laws fl ON fl.id = bfm.foreign_law_id
                WHERE bfm.bill_id = :bid
                ORDER BY bfm.similarity_score DESC
                LIMIT 3
            """),
            {"bid": bill_id},
        ).fetchall()

    return {
        "bill": dict(bill._mapping) if bill else {},
        "stages": [dict(r._mapping) for r in stages],
        "keywords": [dict(r._mapping) for r in keywords],
        "foreign_matches": [dict(r._mapping) for r in foreign_matches],
    }


@st.cache_data(ttl=600)
def get_recent_changes(days: int = 7) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT bill_id, title, chamber, stage_name, stage_date,
                       observed_at, days_ago
                FROM public_marts.mart_bill_timeline
                WHERE days_ago <= :days
                ORDER BY observed_at DESC
                LIMIT 20
            """),
            conn,
            params={"days": days},
        )


@st.cache_data(ttl=600)
def get_dashboard_stats() -> dict:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("""
                SELECT
                    (SELECT count(*) FROM bills WHERE is_passed = false)          AS active_bills,
                    (SELECT count(*) FROM bills
                     WHERE date_introduced >= date_trunc('month', now()))          AS bills_this_month,
                    (SELECT count(*) FROM bills
                     WHERE current_stage IN ('2nd Reading', '3rd Reading'))        AS near_passage,
                    (SELECT count(*) FROM bills
                     WHERE EXISTS (
                         SELECT 1 FROM bill_foreign_matches bfm
                         WHERE bfm.bill_id = bills.bill_id AND bfm.similarity_score >= 20
                     ))                                                             AS foreign_inspired
            """)
        ).fetchone()
    return dict(row._mapping) if row else {}


@st.cache_data(ttl=600)
def get_sponsor_stats() -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("SELECT * FROM public_marts.mart_sponsor_stats ORDER BY bills_introduced DESC LIMIT 50"),
            conn,
        )


@st.cache_data(ttl=600)
def get_pipeline_overview() -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT
                    source,
                    chamber,
                    current_stage,
                    COUNT(*) AS bill_count
                FROM bills
                WHERE current_stage IS NOT NULL AND current_stage != ''
                GROUP BY source, chamber, current_stage
                ORDER BY source, chamber, bill_count DESC
            """),
            conn,
        )


@st.cache_data(ttl=600)
def get_foreign_inspired_bills() -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT
                    b.bill_id, b.title, b.chamber, b.current_stage,
                    b.sponsor, b.date_introduced,
                    COUNT(bfm.id) AS match_count,
                    MAX(bfm.similarity_score) AS top_score,
                    STRING_AGG(fl.jurisdiction || ': ' || fl.law_name, '; '
                               ORDER BY bfm.similarity_score DESC) AS matched_laws
                FROM bills b
                JOIN bill_foreign_matches bfm ON bfm.bill_id = b.bill_id
                JOIN foreign_laws fl ON fl.id = bfm.foreign_law_id
                WHERE bfm.similarity_score >= 20
                GROUP BY b.bill_id, b.title, b.chamber, b.current_stage,
                         b.sponsor, b.date_introduced
                ORDER BY top_score DESC
            """),
            conn,
        )


@st.cache_data(ttl=600)
def search_bills(query: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            text("""
                SELECT b.bill_id, b.title, b.sponsor, b.current_stage,
                       b.date_introduced, b.chamber,
                       s.summary_short,
                       ts_rank(b.search_vector, plainto_tsquery('english', :q)) AS rank
                FROM bills b
                LEFT JOIN bill_summaries s ON s.bill_id = b.bill_id AND s.language = 'en'
                WHERE b.search_vector @@ plainto_tsquery('english', :q)
                   OR s.search_vector @@ plainto_tsquery('english', :q)
                ORDER BY rank DESC
                LIMIT 50
            """),
            conn,
            params={"q": query},
        )
