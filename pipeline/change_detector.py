"""
Status change detector — builds the "Recent Changes" feed.
Compares current bill state against last recorded stage and emits
new bill_stages rows when a transition is detected.
"""
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine, record_stage_if_new
from pipeline.logger import get_logger

log = get_logger("change_detector")

STAGE_ORDER = [
    "Published",
    "1st Reading",
    "Committee Stage",
    "2nd Reading",
    "3rd Reading",
    "Assented",
]


def detect_and_record_changes() -> dict:
    """
    For every bill, check if current_stage has advanced since last recorded stage.
    If it has, insert a new bill_stages row.
    Returns stats dict.
    """
    engine = get_engine()
    stats = {"new_transitions": 0, "bills_checked": 0}

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT b.bill_id, b.current_stage, b.last_updated_at,
                       b.is_passed, b.assent_date,
                       (
                           SELECT bs.stage_name
                           FROM bill_stages bs
                           WHERE bs.bill_id = b.bill_id
                           ORDER BY bs.observed_at DESC
                           LIMIT 1
                       ) AS last_recorded_stage
                FROM bills b
            """)
        ).fetchall()

    stats["bills_checked"] = len(rows)
    log.info(f"Change detection: checking {len(rows)} bills")

    with engine.connect() as conn:
        for bill_id, current_stage, last_updated, is_passed, assent_date, last_recorded in rows:
            if not current_stage:
                continue

            # If current stage != last recorded stage, record the transition
            if current_stage != last_recorded:
                record_stage_if_new(
                    conn, bill_id, current_stage,
                    assent_date if is_passed else date.today(),
                    "change_detector",
                )
                stats["new_transitions"] += 1
                log.info(
                    f"  {bill_id}: stage transition "
                    f"{last_recorded!r} → {current_stage!r}"
                )
        conn.commit()

    log.info(
        f"Change detection complete. "
        f"Checked={stats['bills_checked']} New transitions={stats['new_transitions']}"
    )
    return stats


def get_recent_changes(days: int = 7) -> list[dict]:
    """
    Return bill stage changes observed in the last N days.
    Used by the Streamlit Home page Recent Changes feed.
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    bs.bill_id,
                    b.title,
                    bs.stage_name,
                    bs.stage_date,
                    bs.observed_at,
                    b.chamber
                FROM bill_stages bs
                JOIN bills b ON b.bill_id = bs.bill_id
                WHERE bs.observed_at >= NOW() - INTERVAL ':days days'
                  AND bs.stage_name != 'Published'
                ORDER BY bs.observed_at DESC
                LIMIT 50
            """).bindparams(days=days)
        ).fetchall()

    return [
        {
            "bill_id": r[0],
            "title": r[1],
            "stage_name": r[2],
            "stage_date": r[3],
            "observed_at": r[4],
            "chamber": r[5],
        }
        for r in rows
    ]
