"""
PDF downloader with SHA-256-based deduplication.
Downloads a bill PDF only if its hash has not been stored before.
"""
import hashlib
import os
import time
from typing import Optional

import requests
from sqlalchemy import text

from pipeline.config import settings
from pipeline.db import get_engine
from pipeline.logger import get_logger

log = get_logger("pdf_downloader")

HEADERS = {
    "User-Agent": (
        "BungeWatch/1.0 (+https://github.com/declerke/bungewatch; "
        "Civic data pipeline)"
    )
}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _local_path(bill_id: str, pdf_sha256: str) -> str:
    os.makedirs(settings.PDF_STORAGE_PATH, exist_ok=True)
    return os.path.join(settings.PDF_STORAGE_PATH, f"{bill_id}_{pdf_sha256[:8]}.pdf")


def download_pdf(bill_id: str, pdf_url: str) -> Optional[str]:
    """
    Download a PDF for a bill. Returns local path on success, None on failure.
    Skips download if same hash already exists in raw_bill_pdfs.
    """
    engine = get_engine()

    # Check if already successfully downloaded
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT pdf_sha256, local_path FROM raw_bill_pdfs
                WHERE bill_id = :bill_id AND fetch_status = 'success'
                LIMIT 1
            """),
            {"bill_id": bill_id},
        ).fetchone()
        if row and row[1] and os.path.exists(row[1]):
            log.info(f"  {bill_id}: PDF already on disk, skipping download")
            return row[1]

    # Attempt download
    try:
        log.info(f"  {bill_id}: Downloading {pdf_url}")
        resp = requests.get(pdf_url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()
        content = resp.content
        time.sleep(settings.REQUEST_DELAY_SECONDS)
    except Exception as exc:
        log.warning(f"  {bill_id}: Download failed — {exc}")
        _record_pdf(bill_id, pdf_url, None, None, "failed", str(exc))
        return None

    sha256 = _sha256(content)

    # Check if same content already stored under a different record
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT local_path FROM raw_bill_pdfs WHERE pdf_sha256 = :h LIMIT 1"),
            {"h": sha256},
        ).fetchone()
        if row and row[0] and os.path.exists(row[0]):
            _record_pdf(bill_id, pdf_url, sha256, row[0], "success", None)
            return row[0]

    # Write to disk
    local_path = _local_path(bill_id, sha256)
    with open(local_path, "wb") as f:
        f.write(content)

    _record_pdf(bill_id, pdf_url, sha256, local_path, "success", None)
    log.info(f"  {bill_id}: Saved to {local_path} ({len(content)//1024} KB)")
    return local_path


def _record_pdf(bill_id, pdf_url, sha256, local_path, status, error):
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_bill_pdfs
                    (bill_id, pdf_url, pdf_sha256, local_path, fetched_at, fetch_status, error_message)
                VALUES
                    (:bill_id, :pdf_url, :sha256, :local_path, NOW(), :status, :error)
                ON CONFLICT (bill_id, pdf_url) DO UPDATE SET
                    pdf_sha256    = EXCLUDED.pdf_sha256,
                    local_path    = EXCLUDED.local_path,
                    fetched_at    = NOW(),
                    fetch_status  = EXCLUDED.fetch_status,
                    error_message = EXCLUDED.error_message
            """),
            {
                "bill_id": bill_id, "pdf_url": pdf_url,
                "sha256": sha256, "local_path": local_path,
                "status": status, "error": error,
            },
        )
        conn.commit()


def download_pending_bills() -> dict:
    """Download PDFs for all bills that have a pdf_url but no successful download."""
    engine = get_engine()
    stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT b.bill_id, b.pdf_url
                FROM bills b
                WHERE b.pdf_url IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM raw_bill_pdfs p
                      WHERE p.bill_id = b.bill_id AND p.fetch_status = 'success'
                  )
            """)
        ).fetchall()

    log.info(f"PDFs pending download: {len(rows)}")

    for bill_id, pdf_url in rows:
        path = download_pdf(bill_id, pdf_url)
        if path:
            stats["downloaded"] += 1
        else:
            stats["failed"] += 1

    return stats
