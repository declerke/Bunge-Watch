"""
Standalone pipeline runner — same logic as the Airflow DAG without the scheduler.
Use for testing the full pipeline locally or for a one-shot manual refresh.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.logger import get_logger

log = get_logger("standalone")


def run():
    log.info("═══ BungeWatch standalone pipeline start ═══")

    # 1 & 2: Scrape
    log.info("Step 1: Scraping kenyalaw.org...")
    from scrapers.kenyalaw_scraper import run as kl_run
    kl_run()

    log.info("Step 2: Scraping parliament.go.ke...")
    from scrapers.parliament_scraper import run as parl_run
    parl_run(chamber="NA")

    # 3: Detect changes
    log.info("Step 3: Detecting stage changes...")
    from pipeline.change_detector import detect_and_record_changes
    ch_stats = detect_and_record_changes()

    # 4: Download PDFs
    log.info("Step 4: Downloading PDFs...")
    from pipeline.pdf_downloader import download_pending_bills
    pdf_stats = download_pending_bills()

    # 5: Parse text
    log.info("Step 5: Parsing PDF text...")
    from pipeline.pdf_parser import parse_all_downloaded
    parse_stats = parse_all_downloaded()

    # 6: Keywords
    log.info("Step 6: Extracting keywords...")
    from pipeline.keyword_extractor import enrich_all_bills
    kw_stats = enrich_all_bills()

    # 7: Summaries
    log.info("Step 7: Generating summaries (spaCy extractive)...")
    from pipeline.claude_summarizer import summarise_all_bills
    sum_stats = summarise_all_bills()

    # 8: Foreign law matching
    log.info("Step 8: Comparing to foreign laws...")
    from pipeline.foreign_law_matcher import match_all_bills
    fl_stats = match_all_bills()

    log.info("═══ Pipeline complete ═══")
    log.info(f"  Stage changes detected : {ch_stats.get('new_transitions', 0)}")
    log.info(f"  PDFs downloaded        : {pdf_stats.get('downloaded', 0)}")
    log.info(f"  PDFs parsed            : {parse_stats.get('parsed', 0)}")
    log.info(f"  Keywords enriched      : {kw_stats.get('enriched', 0)}")
    log.info(f"  Summaries created      : {sum_stats.get('summarised', 0)}")
    log.info(f"  Foreign matches stored : {fl_stats.get('matched', 0)}")
    log.info("Run `dbt run && dbt test` in dbt/ to refresh marts.")


if __name__ == "__main__":
    run()
