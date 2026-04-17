"""Database engine, session factory, and upsert helpers."""
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from pipeline.config import settings

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            future=True,
        )
    return _engine


def get_session_factory():
    return sessionmaker(get_engine())


@contextmanager
def session_scope():
    Session = get_session_factory()
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_bill(conn, bill: dict[str, Any]) -> str:
    """Insert or update a bill row. Returns 'inserted' or 'updated'."""
    result = conn.execute(
        text("""
            INSERT INTO bills (
                bill_id, source, bill_number, title, sponsor, sponsor_party,
                chamber, date_introduced, gazette_no, current_stage,
                is_passed, assent_date, source_url, pdf_url, text_sha256
            ) VALUES (
                :bill_id, :source, :bill_number, :title, :sponsor, :sponsor_party,
                :chamber, :date_introduced, :gazette_no, :current_stage,
                :is_passed, :assent_date, :source_url, :pdf_url, :text_sha256
            )
            ON CONFLICT (bill_id) DO UPDATE SET
                current_stage   = EXCLUDED.current_stage,
                is_passed       = EXCLUDED.is_passed,
                assent_date     = EXCLUDED.assent_date,
                sponsor         = COALESCE(EXCLUDED.sponsor, bills.sponsor),
                pdf_url         = COALESCE(EXCLUDED.pdf_url, bills.pdf_url),
                gazette_no      = COALESCE(EXCLUDED.gazette_no, bills.gazette_no),
                last_updated_at = NOW()
            RETURNING (xmax = 0) AS is_new
        """),
        bill,
    )
    row = result.fetchone()
    return "inserted" if row and row[0] else "updated"


def record_stage_if_new(conn, bill_id: str, stage_name: str, stage_date=None, source: str = None):
    """Record a stage transition only if this exact stage hasn't been recorded yet."""
    conn.execute(
        text("""
            INSERT INTO bill_stages (bill_id, stage_name, stage_date, source)
            SELECT :bill_id, :stage_name, :stage_date, :source
            WHERE NOT EXISTS (
                SELECT 1 FROM bill_stages
                WHERE bill_id = :bill_id AND stage_name = :stage_name
            )
        """),
        {"bill_id": bill_id, "stage_name": stage_name,
         "stage_date": stage_date, "source": source},
    )
