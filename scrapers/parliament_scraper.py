"""
Parliament of Kenya scraper (parliament.go.ke).
Source: JS-rendered Drupal Views — Playwright intercepts AJAX responses.
Extracts bill metadata and PDF links from the National Assembly bills page.
"""
import json
import re
import time
from typing import Optional

from sqlalchemy import text

from pipeline.config import settings
from pipeline.db import get_engine, upsert_bill, record_stage_if_new
from pipeline.logger import get_logger, scrape_run
from scrapers.selectors import PARLIAMENT

log = get_logger("parliament_scraper")


def _make_bill_id(title: str, year: int) -> str:
    """Stable ID from bill title slug + year."""
    slug = re.sub(r"[^a-zA-Z0-9]", "", title[:40])
    return f"PARL-{slug}-{year}".upper()


def _extract_year_from_title(title: str) -> int:
    """Extract year from 'The Foo Bar Bill, 2026' style titles."""
    match = re.search(r",\s*(20\d{2})", title)
    if match:
        return int(match.group(1))
    from datetime import date
    return date.today().year


def _parse_bill_items(page, base_url: str, chamber: str) -> list[dict]:
    """Extract all bill dicts from the current Playwright page state."""
    bills = []
    for item in page.query_selector_all(PARLIAMENT["bill_items"]):
        title_el = item.query_selector(PARLIAMENT["bill_title_link"])
        if not title_el:
            continue

        raw_title = title_el.inner_text().strip()
        title = raw_title[:-4].strip() if raw_title.lower().endswith(".pdf") else raw_title
        if not title:
            continue

        href = title_el.get_attribute("href") or ""
        if href and not href.startswith("http"):
            href = f"{base_url}{href}"
        pdf_url = href if href.lower().endswith(".pdf") else None

        digest_el = item.query_selector(PARLIAMENT["bill_digest_link"])
        tracker_el = item.query_selector(PARLIAMENT["bill_tracker_link"])
        digest_href = digest_el.get_attribute("href") if digest_el else None
        tracker_href = tracker_el.get_attribute("href") if tracker_el else None
        if digest_href and not digest_href.startswith("http"):
            digest_href = f"{base_url}{digest_href}"
        if tracker_href and not tracker_href.startswith("http"):
            tracker_href = f"{base_url}{tracker_href}"

        source_url = digest_href or tracker_href or pdf_url or base_url
        year = _extract_year_from_title(title)
        bill_id = _make_bill_id(title, year)

        bills.append({
            "bill_id": bill_id,
            "source": "parliament",
            "bill_number": None,
            "title": title,
            "sponsor": None,
            "sponsor_party": None,
            "chamber": chamber,
            "date_introduced": None,
            "gazette_no": None,
            "current_stage": "Published",
            "is_passed": False,
            "assent_date": None,
            "source_url": source_url,
            "pdf_url": pdf_url,
            "text_sha256": None,
        })
    return bills


def _get_last_page_number(page) -> int:
    """Return the 0-based index of the last page (0 if no pagination)."""
    import re
    last_link = page.query_selector("li.pager__item--last a")
    if not last_link:
        return 0
    href = last_link.get_attribute("href") or ""
    m = re.search(r"[?&]page=(\d+)", href)
    return int(m.group(1)) if m else 0


def _wait_for_bills(page, timeout_ms: int = 30000) -> bool:
    """Wait for bill content to appear. Returns True if found, False on timeout."""
    try:
        page.wait_for_selector(PARLIAMENT["bill_items"], timeout=timeout_ms)
        return True
    except Exception:
        return False


def _build_page_url(base_url: str, path: str, page_num: int = 0) -> str:
    """Build a filtered URL for the 13th Parliament (2022 session)."""
    url = f"{base_url}{path}?title=%20&field_parliament_value=2022"
    if page_num > 0:
        url += f"&page={page_num}"
    return url


def scrape_bills_page(chamber: str = "NA") -> list[dict]:
    """Scrape ALL paginated bills from the parliament.go.ke bills listing."""
    from playwright.sync_api import sync_playwright

    path = (PARLIAMENT["bills_path"] if chamber == "NA"
            else PARLIAMENT["senate_bills_path"])
    base_url = settings.PARLIAMENT_BASE_URL
    all_bills: list[dict] = []
    seen_ids: set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page()

        # ── Page 0: load with 13th Parliament filter, wait for DOM content ────
        first_url = _build_page_url(base_url, path, 0)
        log.info(f"Loading parliament.go.ke bills page: {first_url}")
        page.goto(first_url, wait_until="domcontentloaded", timeout=60000)

        found = _wait_for_bills(page)
        if not found:
            log.warning(f"  Selector '{PARLIAMENT['bill_items']}' not found after 30s — page may be empty or structure changed")

        last_page = _get_last_page_number(page)
        log.info(f"  Pagination: pages 0–{last_page} ({last_page + 1} total)")

        page_bills = _parse_bill_items(page, base_url, chamber)
        log.info(f"  Found {len(page_bills)} bills on page 0")
        for b in page_bills:
            if b["bill_id"] not in seen_ids:
                all_bills.append(b)
                seen_ids.add(b["bill_id"])

        # ── Pages 1 … last_page ───────────────────────────────────────────────
        for p in range(1, last_page + 1):
            page_url = _build_page_url(base_url, path, p)
            log.info(f"  Loading page {p + 1}: {page_url}")
            page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
            _wait_for_bills(page)
            time.sleep(settings.REQUEST_DELAY_SECONDS)

            page_bills = _parse_bill_items(page, base_url, chamber)
            log.info(f"  Found {len(page_bills)} bills on page {p}")
            for b in page_bills:
                if b["bill_id"] not in seen_ids:
                    all_bills.append(b)
                    seen_ids.add(b["bill_id"])

        browser.close()

    log.info(f"Parliament {chamber}: {len(all_bills)} unique bills across all pages")
    return all_bills


def run(chamber: str = "NA"):
    """Main entry point: scrape parliament bills and persist to DB."""
    engine = get_engine()

    with scrape_run(f"parliament_{chamber}") as stats:
        bills = scrape_bills_page(chamber)

        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO raw_parliament_scrapes (page_type, raw_json)
                    VALUES (:page_type, :raw_json)
                """),
                {
                    "page_type": f"bills_{chamber}",
                    "raw_json": json.dumps(
                        [{"title": b["title"], "source_url": b["source_url"],
                          "pdf_url": b["pdf_url"]} for b in bills]
                    ),
                },
            )

            for bill in bills:
                outcome = upsert_bill(conn, bill)
                record_stage_if_new(conn, bill["bill_id"], "Published", None, "parliament")
                if outcome == "inserted":
                    stats["records_ingested"] += 1
                else:
                    stats["records_updated"] += 1

            conn.commit()
        log.info(f"Parliament {chamber} scrape committed.")


if __name__ == "__main__":
    run()
