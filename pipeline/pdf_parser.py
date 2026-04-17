"""
PDF text extractor.
Primary: pdfplumber (handles text-layer PDFs well).
Fallback: PyMuPDF/fitz (handles more edge cases).
Logs parse_failed without crashing the pipeline.
"""
import hashlib
import os
from typing import Optional

from sqlalchemy import text

from pipeline.db import get_engine
from pipeline.logger import get_logger

log = get_logger("pdf_parser")


def _sha256_text(text_content: str) -> str:
    return hashlib.sha256(text_content.encode("utf-8")).hexdigest()


def extract_with_pdfplumber(path: str) -> Optional[str]:
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
        return "\n\n".join(pages) if pages else None
    except Exception as exc:
        log.debug(f"pdfplumber failed on {path}: {exc}")
        return None


def extract_with_pymupdf(path: str) -> Optional[str]:
    try:
        import fitz
        doc = fitz.open(path)
        pages = [page.get_text() for page in doc]
        doc.close()
        text = "\n\n".join(p for p in pages if p.strip())
        return text if text.strip() else None
    except Exception as exc:
        log.debug(f"PyMuPDF failed on {path}: {exc}")
        return None


def diagnose_pdf(bill_id: str, path: str):
    """Log PDF internals to explain why both parsers returned empty."""
    try:
        import fitz
        doc = fitz.open(path)
        encrypted = doc.is_encrypted
        needs_pass = doc.needs_pass
        page_count = doc.page_count
        raw_sample = ""
        if page_count > 0:
            raw_sample = repr(doc[0].get_text()[:200])
        image_count = sum(len(doc[i].get_images()) for i in range(min(3, page_count)))
        doc.close()
        log.info(
            f"  {bill_id}: PDF diagnosis — pages={page_count}, "
            f"encrypted={encrypted}, needs_pass={needs_pass}, "
            f"images_in_first_3_pages={image_count}, "
            f"raw_text_sample={raw_sample}"
        )
    except Exception as exc:
        log.info(f"  {bill_id}: PDF diagnosis failed — {exc}")


def extract_with_ocr(path: str) -> Optional[str]:
    """OCR fallback for image-based (scanned) PDFs using Tesseract.
    Processes one page at a time at 150 DPI to keep memory usage flat.
    """
    try:
        import pytesseract
        from pdf2image import convert_from_path, pdfinfo_from_path
        info = pdfinfo_from_path(path)
        page_count = info.get("Pages", 0)
        if not page_count:
            return None

        pages = []
        for page_num in range(1, page_count + 1):
            imgs = convert_from_path(path, dpi=150, first_page=page_num, last_page=page_num)
            if not imgs:
                continue
            page_text = pytesseract.image_to_string(imgs[0], lang="eng")
            imgs[0].close()
            if page_text.strip():
                pages.append(page_text)

        text = "\n\n".join(pages)
        return text if text.strip() else None
    except Exception as exc:
        log.debug(f"OCR failed on {path}: {exc}")
        return None


def parse_pdf(bill_id: str, local_path: str) -> Optional[str]:
    """
    Parse a PDF file and store the extracted text.
    Returns extracted text or None if all parsers failed.
    """
    if not os.path.exists(local_path):
        log.warning(f"  {bill_id}: PDF not found at {local_path}")
        _record_text(bill_id, None, "failed", "file_not_found")
        return None

    # Try pdfplumber first
    text = extract_with_pdfplumber(local_path)
    parser_used = "pdfplumber"

    if not text:
        log.info(f"  {bill_id}: pdfplumber empty, trying PyMuPDF")
        text = extract_with_pymupdf(local_path)
        parser_used = "pymupdf"

    if not text:
        log.info(f"  {bill_id}: PyMuPDF empty, trying OCR (scanned PDF)")
        text = extract_with_ocr(local_path)
        parser_used = "tesseract"

    if not text:
        diagnose_pdf(bill_id, local_path)
        log.warning(f"  {bill_id}: All parsers failed — marking as parse_failed")
        _record_text(bill_id, None, "failed", None)
        return None

    char_count = len(text)
    log.info(f"  {bill_id}: Extracted {char_count:,} chars via {parser_used}")
    _record_text(bill_id, text, "success", parser_used)

    # Update bills.text_sha256
    sha = _sha256_text(text)
    _update_bill_hash(bill_id, sha)

    return text


def _record_text(bill_id: str, full_text: Optional[str], status: str, parser: Optional[str]):
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_bill_text (bill_id, full_text, char_count, parse_status, parser_used)
                VALUES (:bill_id, :full_text, :char_count, :status, :parser)
                ON CONFLICT (bill_id) DO UPDATE SET
                    full_text    = EXCLUDED.full_text,
                    char_count   = EXCLUDED.char_count,
                    parse_status = EXCLUDED.parse_status,
                    parser_used  = EXCLUDED.parser_used,
                    extracted_at = NOW()
            """),
            {
                "bill_id": bill_id,
                "full_text": full_text,
                "char_count": len(full_text) if full_text else 0,
                "status": status,
                "parser": parser,
            },
        )
        conn.commit()


def _update_bill_hash(bill_id: str, sha256: str):
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(
            text("UPDATE bills SET text_sha256 = :sha WHERE bill_id = :bid"),
            {"sha": sha256, "bid": bill_id},
        )
        conn.commit()


def parse_all_downloaded(years: list[int] | None = None) -> dict:
    """Parse PDFs for all bills with a successful download but no text yet.
    Uses 4 parallel workers — each processes one PDF at a time (page-by-page
    internally) so RAM stays flat regardless of PDF size.

    Args:
        years: If provided, only parse bills whose bill_id ends with one of
               these years (e.g. [2025, 2026]). Filters by the year suffix
               in the PARL-xxx-YYYY bill_id pattern.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    engine = get_engine()
    stats = {"parsed": 0, "failed": 0, "skipped": 0}
    lock = threading.Lock()

    year_filter = ""
    year_params: dict = {}
    if years:
        clauses = " OR ".join(f"p.bill_id LIKE :y{i}" for i, _ in enumerate(years))
        year_filter = f"AND ({clauses})"
        year_params = {f"y{i}": f"%-{y}" for i, y in enumerate(years)}

    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT p.bill_id, p.local_path
                FROM raw_bill_pdfs p
                JOIN bills b ON b.bill_id = p.bill_id
                WHERE p.fetch_status = 'success'
                  AND b.source = 'parliament'
                  AND NOT EXISTS (
                      SELECT 1 FROM raw_bill_text t
                      WHERE t.bill_id = p.bill_id AND t.parse_status = 'success'
                  )
                  {year_filter}
                ORDER BY p.bill_id
            """),
            year_params,
        ).fetchall()

    log.info(f"PDFs pending text extraction: {len(rows)} (4 parallel workers)")

    def _process_one(bill_id: str, local_path: str):
        if local_path and os.path.exists(local_path):
            result = parse_pdf(bill_id, local_path)
            with lock:
                if result:
                    stats["parsed"] += 1
                else:
                    stats["failed"] += 1
        else:
            with lock:
                stats["skipped"] += 1

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_process_one, bill_id, local_path): bill_id
            for bill_id, local_path in rows
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                bill_id = futures[future]
                log.warning(f"  {bill_id}: thread error — {exc}")
                with lock:
                    stats["failed"] += 1

    return stats
