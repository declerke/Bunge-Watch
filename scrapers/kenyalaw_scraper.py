"""
Kenya Law scraper (kenyalaw.org).
Source: static HTML table — requests + BeautifulSoup4.
Fetches one year at a time; URL pattern: ?id=12043&yr=YYYY
"""
import hashlib
import re
import time
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from pipeline.config import settings
from pipeline.db import get_engine, upsert_bill, record_stage_if_new
from pipeline.logger import get_logger, scrape_run
from scrapers.selectors import KENYALAW

log = get_logger("kenyalaw_scraper")

HEADERS = {
    "User-Agent": (
        "BungeWatch/1.0 (+https://github.com/declerke/bungewatch; "
        "Civic data pipeline - parliamentary bill tracker)"
    )
}


def _make_bill_id(bill_number: str, year: int) -> str:
    """Stable, collision-free bill ID from the bill number.

    Kenyan bill numbers already embed the year (e.g. 'No. 52 of 2024'),
    so we must NOT append the scraping-year — that caused the same bill to
    get three distinct IDs when Kenya Law lists it on multiple year pages.
    The ``year`` arg is used only as a fallback for unnumbered bills.
    """
    clean = re.sub(r"[^a-zA-Z0-9]", "", bill_number or "")
    if not clean:
        clean = f"UNKNOWN{year}"
    return f"KL-{clean}".upper()


def _parse_date(raw: str) -> Optional[date]:
    """Parse dd/mm/yy or dd/mm/yyyy dates."""
    raw = raw.strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_bill_row(cells: list, year: int) -> Optional[dict]:
    """Parse a single table row into a bill dict. Returns None if row is a header."""
    if len(cells) < 7:
        return None

    title_cell = cells[KENYALAW["col_title"]]
    title_tag = title_cell.find("strong") or title_cell.find("b")
    title = (title_tag.get_text(strip=True) if title_tag
             else title_cell.get_text(strip=True))
    if not title or title.lower() in ("bill", "title"):
        return None

    pdf_anchor = title_cell.find("a", href=lambda h: h and ".pdf" in h.lower())
    pdf_url = pdf_anchor["href"] if pdf_anchor else None
    if pdf_url and not pdf_url.startswith("http"):
        base = settings.KENYALAW_BASE_URL.split("/kl/")[0]
        pdf_url = f"{base}{pdf_url}"

    bill_number = cells[KENYALAW["col_bill_number"]].get_text(strip=True)
    sponsor = cells[KENYALAW["col_sponsor"]].get_text(strip=True) or None
    date_raw = cells[KENYALAW["col_date"]].get_text(strip=True)
    gazette = cells[KENYALAW["col_gazette"]].get_text(strip=True) or None

    first_read = cells[KENYALAW["col_first_read"]].get_text(strip=True) or None
    second_read = (
        cells[KENYALAW["col_second_read"]].get_text(strip=True)
        if len(cells) > KENYALAW["col_second_read"] else None
    )
    third_read = (
        cells[KENYALAW["col_third_read"]].get_text(strip=True)
        if len(cells) > KENYALAW["col_third_read"] else None
    )
    assent_raw = (
        cells[KENYALAW["col_assent"]].get_text(strip=True)
        if len(cells) > KENYALAW["col_assent"] else None
    )

    current_stage = "Published"
    is_passed = False
    assent_date = None

    if assent_raw and re.search(r"\d", assent_raw):
        current_stage = "Assented"
        is_passed = True
        assent_date = _parse_date(assent_raw)
    elif third_read and re.search(r"\d", third_read):
        current_stage = "3rd Reading"
    elif second_read and re.search(r"\d", second_read):
        current_stage = "2nd Reading"
    elif first_read and re.search(r"\d", first_read):
        current_stage = "1st Reading"

    bill_id = _make_bill_id(bill_number, year)
    source_url = f"{settings.KENYALAW_BASE_URL}?id=12043&yr={year}"

    return {
        "bill_id": bill_id,
        "source": "kenyalaw",
        "bill_number": bill_number or None,
        "title": title,
        "sponsor": sponsor,
        "sponsor_party": None,
        "chamber": "NA",
        "date_introduced": _parse_date(date_raw),
        "gazette_no": gazette,
        "current_stage": current_stage,
        "is_passed": is_passed,
        "assent_date": assent_date,
        "source_url": source_url,
        "pdf_url": pdf_url,
        "text_sha256": None,
        "_stages": {
            "1st Reading": _parse_date(first_read) if first_read else None,
            "2nd Reading": _parse_date(second_read) if second_read else None,
            "3rd Reading": _parse_date(third_read) if third_read else None,
            "Assented": assent_date,
        },
    }


def fetch_year(year: int) -> list[dict]:
    """Fetch all bills for a given year from kenyalaw.org."""
    url = f"{settings.KENYALAW_BASE_URL}?id=12043&yr={year}"
    log.info(f"Fetching kenyalaw.org year={year}: {url}")

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    time.sleep(settings.REQUEST_DELAY_SECONDS)

    soup = BeautifulSoup(resp.text, "lxml")
    rows = soup.select(KENYALAW["bill_rows"])
    bills = []
    for row in rows:
        cells = row.find_all("td")
        parsed = _parse_bill_row(cells, year)
        if parsed:
            bills.append(parsed)

    log.info(f"  Parsed {len(bills)} bills for {year}")
    return bills, resp.text


def run(years: Optional[list[int]] = None):
    """Main entry point: scrape all years and persist to DB."""
    years = years or settings.SCRAPE_YEARS
    engine = get_engine()

    with scrape_run("kenyalaw") as stats:
        for year in years:
            bills, raw_html = fetch_year(year)

            with engine.connect() as conn:
                # Save raw HTML snapshot
                conn.execute(
                    text("""
                        INSERT INTO raw_kenyalaw_scrapes (year, raw_html, record_count)
                        VALUES (:year, :raw_html, :count)
                    """),
                    {"year": year, "raw_html": raw_html, "count": len(bills)},
                )

                for bill in bills:
                    stages = bill.pop("_stages")
                    outcome = upsert_bill(conn, bill)

                    for stage_name, stage_date in stages.items():
                        if stage_date or stage_name in ("1st Reading",):
                            record_stage_if_new(
                                conn, bill["bill_id"], stage_name, stage_date, "kenyalaw"
                            )

                    if outcome == "inserted":
                        stats["records_ingested"] += 1
                    else:
                        stats["records_updated"] += 1

                conn.commit()
            log.info(f"  Year {year} committed.")


if __name__ == "__main__":
    run()
