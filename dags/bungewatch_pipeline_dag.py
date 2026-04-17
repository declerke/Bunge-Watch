"""
BungeWatch Kenya — Daily Legislative Intelligence Pipeline
Airflow DAG: 10 tasks, runs daily at 06:00 Africa/Nairobi.

Task graph:
    scrape_kenyalaw ─┐
                     ├─► consolidate_and_detect ─► download_pdfs ─► parse_pdf_text
    scrape_parliament┘       ─► extract_keywords ─► generate_summaries
                             ─► compare_foreign_laws ─► dbt_run ─► dbt_test ─► log_summary
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# Ensure pipeline and scrapers are importable from /opt/airflow
sys.path.insert(0, "/opt/airflow")

DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "bungewatch",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="bungewatch_pipeline",
    description="Daily scrape, PDF parse, NLP enrich, dbt transform for Kenyan parliamentary bills",
    schedule="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["bungewatch", "civic", "legislative"],
) as dag:

    # ── 1 & 2: Scrape sources in parallel ─────────────────────────────────────

    def _scrape_kenyalaw(**ctx):
        from scrapers.kenyalaw_scraper import run
        run()

    def _scrape_parliament(**ctx):
        from scrapers.parliament_scraper import run
        run(chamber="NA")

    t_scrape_kenyalaw = PythonOperator(
        task_id="scrape_kenyalaw",
        python_callable=_scrape_kenyalaw,
    )

    t_scrape_parliament = PythonOperator(
        task_id="scrape_parliament",
        python_callable=_scrape_parliament,
    )

    # ── 3: Detect stage changes ────────────────────────────────────────────────

    def _detect_changes(**ctx):
        from pipeline.change_detector import detect_and_record_changes
        stats = detect_and_record_changes()
        ctx["ti"].xcom_push(key="change_stats", value=stats)

    t_detect_changes = PythonOperator(
        task_id="detect_changes",
        python_callable=_detect_changes,
    )

    # ── 4: Download PDFs ───────────────────────────────────────────────────────

    def _download_pdfs(**ctx):
        from pipeline.pdf_downloader import download_pending_bills
        stats = download_pending_bills()
        ctx["ti"].xcom_push(key="pdf_stats", value=stats)

    t_download_pdfs = PythonOperator(
        task_id="download_pdfs",
        python_callable=_download_pdfs,
    )

    # ── 5: Parse PDF text ──────────────────────────────────────────────────────

    def _parse_pdfs(**ctx):
        from pipeline.pdf_parser import parse_all_downloaded
        stats = parse_all_downloaded()
        ctx["ti"].xcom_push(key="parse_stats", value=stats)

    t_parse_pdfs = PythonOperator(
        task_id="parse_pdf_text",
        python_callable=_parse_pdfs,
    )

    # ── 6: Extract keywords ────────────────────────────────────────────────────

    def _extract_keywords(**ctx):
        from pipeline.keyword_extractor import enrich_all_bills
        stats = enrich_all_bills()
        ctx["ti"].xcom_push(key="keyword_stats", value=stats)

    t_extract_keywords = PythonOperator(
        task_id="extract_keywords",
        python_callable=_extract_keywords,
    )

    # ── 7: Generate Claude summaries ───────────────────────────────────────────

    def _generate_summaries(**ctx):
        from pipeline.claude_summarizer import summarise_all_bills
        stats = summarise_all_bills()
        ctx["ti"].xcom_push(key="summary_stats", value=stats)

    t_generate_summaries = PythonOperator(
        task_id="generate_summaries",
        python_callable=_generate_summaries,
    )

    # ── 8: Foreign law comparison ──────────────────────────────────────────────

    def _compare_foreign_laws(**ctx):
        from pipeline.foreign_law_matcher import match_all_bills
        stats = match_all_bills()
        ctx["ti"].xcom_push(key="foreign_stats", value=stats)

    t_compare_foreign = PythonOperator(
        task_id="compare_foreign_laws",
        python_callable=_compare_foreign_laws,
    )

    # ── 9: dbt run ─────────────────────────────────────────────────────────────

    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} "
            f"{DBT_BIN} deps --quiet && "
            f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} "
            f"{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "bungewatch"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "bungewatch"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "bungewatch"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "PATH": os.environ.get("PATH", ""),
        },
        append_env=True,
    )

    # ── 10: dbt test ───────────────────────────────────────────────────────────

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"DBT_PROFILES_DIR={DBT_PROFILES_DIR} "
            f"{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env={
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "bungewatch"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "bungewatch"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "bungewatch"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "PATH": os.environ.get("PATH", ""),
        },
        append_env=True,
    )

    # ── 11: Log summary ────────────────────────────────────────────────────────

    def _log_summary(**ctx):
        ti = ctx["ti"]
        pdf_stats   = ti.xcom_pull(task_ids="download_pdfs",       key="pdf_stats")    or {}
        parse_stats = ti.xcom_pull(task_ids="parse_pdf_text",       key="parse_stats")  or {}
        kw_stats    = ti.xcom_pull(task_ids="extract_keywords",     key="keyword_stats") or {}
        sum_stats   = ti.xcom_pull(task_ids="generate_summaries",   key="summary_stats") or {}
        fl_stats    = ti.xcom_pull(task_ids="compare_foreign_laws", key="foreign_stats") or {}
        ch_stats    = ti.xcom_pull(task_ids="detect_changes",       key="change_stats")  or {}

        from pipeline.logger import get_logger
        log = get_logger("dag_summary")
        log.info("═══════════════════════════════════════")
        log.info("BungeWatch pipeline run complete")
        log.info(f"  PDFs downloaded  : {pdf_stats.get('downloaded', 0)}")
        log.info(f"  PDFs parsed      : {parse_stats.get('parsed', 0)}")
        log.info(f"  Keywords enriched: {kw_stats.get('enriched', 0)}")
        log.info(f"  Summaries created: {sum_stats.get('summarised', 0)}")
        log.info(f"  Foreign matches  : {fl_stats.get('matched', 0)}")
        log.info(f"  Stage changes    : {ch_stats.get('new_transitions', 0)}")
        log.info("═══════════════════════════════════════")

    t_log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=_log_summary,
        trigger_rule="all_done",
    )

    # ── Task dependencies ──────────────────────────────────────────────────────
    [t_scrape_kenyalaw, t_scrape_parliament] >> t_detect_changes
    t_detect_changes >> t_download_pdfs >> t_parse_pdfs
    t_parse_pdfs >> t_extract_keywords >> t_generate_summaries
    t_generate_summaries >> t_compare_foreign
    t_compare_foreign >> t_dbt_run >> t_dbt_test >> t_log_summary
