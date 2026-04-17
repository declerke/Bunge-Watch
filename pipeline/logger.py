"""Structured pipeline logger — writes to scrape_runs table + stdout."""
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


@contextmanager
def scrape_run(source: str):
    """Context manager that records a scrape run in the scrape_runs table."""
    engine = get_engine()
    run_id: Optional[int] = None
    started = datetime.now(timezone.utc)

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                INSERT INTO scrape_runs (source, started_at, status)
                VALUES (:source, :started_at, 'running')
                RETURNING id
            """),
            {"source": source, "started_at": started},
        )
        run_id = result.scalar()
        conn.commit()

    stats = {"records_ingested": 0, "records_updated": 0}
    log = get_logger(source)
    log.info(f"Run #{run_id} started for source={source}")

    try:
        yield stats
        with engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE scrape_runs SET
                        finished_at      = NOW(),
                        status           = 'success',
                        records_ingested = :ingested,
                        records_updated  = :updated
                    WHERE id = :run_id
                """),
                {
                    "ingested": stats["records_ingested"],
                    "updated": stats["records_updated"],
                    "run_id": run_id,
                },
            )
            conn.commit()
        log.info(
            f"Run #{run_id} SUCCESS — "
            f"ingested={stats['records_ingested']} updated={stats['records_updated']}"
        )
    except Exception as exc:
        with engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE scrape_runs SET
                        finished_at   = NOW(),
                        status        = 'failed',
                        error_message = :error
                    WHERE id = :run_id
                """),
                {"error": str(exc)[:500], "run_id": run_id},
            )
            conn.commit()
        log.error(f"Run #{run_id} FAILED — {exc}")
        raise
