# BungeWatch Kenya

**Daily legislative intelligence pipeline for Kenya's Parliament — scrapes, parses, and NLP-enriches every bill so citizens and analysts can track what Bunge is doing.**

---

## TL;DR

BungeWatch automatically scrapes bills from two official Kenyan sources (KenyaLaw and Parliament of Kenya), downloads PDFs, extracts full text using a three-tier parser (pdfplumber → PyMuPDF → Tesseract OCR for scanned bills), enriches each bill with NLP keywords and a plain-language summary, compares bills against a curated foreign-law reference corpus, and surfaces everything in a Streamlit dashboard with dbt-powered analytics.

| Metric | Value |
|--------|-------|
| Bills tracked | 319 |
| PDFs downloaded | 319 |
| Bills parsed (text extracted) | 124 |
| Keywords extracted | 1,220 |
| Bill summaries generated | 122 |
| Foreign law comparisons | 4 bills matched |
| dbt models | 8 (2 staging · 2 intermediate · 4 marts) |
| dbt tests | 28 passing |
| Airflow DAG tasks | 11 |
| Dashboard pages | 4 |

---

## Architecture

```
KenyaLaw (BS4)          Parliament (Playwright)
      │                          │
      └──────────┬───────────────┘
                 ▼
         detect_changes
                 ▼
         download_pdfs  (requests + retry)
                 ▼
         parse_pdf_text
            pdfplumber
               └─► PyMuPDF
                     └─► Tesseract OCR  (scanned/image PDFs)
                 ▼
         extract_keywords  (YAKE + spaCy)
                 ▼
         generate_summaries  (spaCy extractive)
                 ▼
         compare_foreign_laws  (TF-IDF cosine similarity)
                 ▼
         dbt run / dbt test  (8 models, 28 tests)
                 ▼
         Streamlit Dashboard  (4 pages)
```

All stages run as an Airflow 3.0 DAG on a daily schedule (06:00 Africa/Nairobi).

---

## Data Sources

| Source | Method | Coverage |
|--------|--------|----------|
| [KenyaLaw](https://kenyalaw.org) | BeautifulSoup scraper | National Assembly + Senate bills |
| [Parliament of Kenya](https://parliament.go.ke) | Playwright (JS-rendered Drupal Views) | 13th Parliament (2022–) bills |
| Foreign Laws corpus | Seeded CSV (15 reference laws) | Uganda, Tanzania, South Africa, UK, India |

---

## Pipeline Stages

1. **Scrape** — Two scrapers run in parallel. KenyaLaw uses BS4 against paginated HTML. Parliament.go.ke requires Playwright because bill listings are rendered by Drupal Views via AJAX.

2. **Detect changes** — Compares current bill metadata against the DB to record new stage transitions (e.g., _First Reading → Second Reading_), preventing duplicate stage events.

3. **Download PDFs** — Fetches each bill PDF with retries, stores to `/opt/airflow/data/pdfs/`, records `fetch_status` per bill.

4. **Parse PDF text** — Three-tier extraction with automatic fallback:
   - **pdfplumber** — fast, handles text-layer PDFs well
   - **PyMuPDF / fitz** — better edge-case coverage on complex layouts
   - **Tesseract OCR** — page-by-page at 150 DPI for fully scanned bills; 4 parallel workers keep RAM flat regardless of PDF size

5. **Extract keywords** — YAKE (Yet Another Keyword Extractor) scores candidates; spaCy NER filters noise; top 10 keywords stored per bill.

6. **Generate summaries** — spaCy sentence-rank extractive summarizer produces a 3–5 sentence plain-language summary from the full bill text. Runs fully offline with no API cost.

7. **Compare foreign laws** — TF-IDF vectors for each bill compared against 15 seeded foreign reference laws via cosine similarity; matches above threshold are stored for the dashboard.

8. **dbt run / test** — 8 dbt models transform raw tables into clean marts; 28 data quality tests verify referential integrity, non-null constraints, and value ranges.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 3.0 (LocalExecutor) |
| Scraping | Playwright (Chromium headless), BeautifulSoup4 |
| PDF parsing | pdfplumber, PyMuPDF (fitz), Tesseract OCR, pdf2image |
| NLP | spaCy (`en_core_web_sm`), YAKE, TF-IDF (scikit-learn) |
| Data storage | PostgreSQL 15 |
| Data transformation | dbt-core + dbt-postgres |
| Dashboard | Streamlit |
| Containerisation | Docker Compose (3 services: postgres, airflow, streamlit) |
| Language | Python 3.11 |

---

## Key Design Decisions

**Three-tier PDF parser with graceful fallback**
Kenya's Parliament publishes a mix of text-layer PDFs (newer bills) and fully scanned image PDFs (older bills). Using only pdfplumber would silently return empty for ~40% of bills. The cascade (pdfplumber → PyMuPDF → Tesseract) ensures maximum coverage, and the `diagnose_pdf` function logs PDF internals (page count, encryption, image density) when all three parsers fail so failures are explainable.

**Page-by-page OCR at 150 DPI instead of bulk conversion**
Loading an entire 300-page scanned bill into memory at 200 DPI would require ~2 GB RAM per worker and caused OOM kills. Processing one page at a time with `convert_from_path(first_page=N, last_page=N)` at 150 DPI keeps peak RAM below 200 MB per worker regardless of bill size.

**4-worker ThreadPoolExecutor for parallel OCR**
Tesseract is CPU-bound but Airflow runs in a single Python process. Using `ThreadPoolExecutor(max_workers=4)` saturates the available CPU cores without spawning separate processes, avoiding inter-process coordination overhead while achieving near-linear speedup for the OCR workload.

**Offline NLP with no API dependency**
Summaries and keyword extraction use spaCy + YAKE — both run entirely locally. This means the pipeline can run indefinitely with zero API cost, and the ANTHROPIC_API_KEY in `.env.example` is genuinely optional. Future versions could swap in Claude for richer summaries without changing the interface.

**Playwright for Parliament.go.ke instead of direct HTTP**
The bills listing page is rendered by Drupal Views via AJAX — a plain `requests.get` returns an empty table. Playwright intercepts the fully rendered DOM after JavaScript executes, making it the only reliable scraping approach without reverse-engineering private API endpoints.

**dbt as the single source of truth for analytics tables**
Rather than writing SQL analytics directly in Streamlit queries, all mart tables are produced by dbt. This enforces column-level documentation, automated tests, and a lineage graph; the Streamlit app only reads from mart views, never raw tables.

---

## Project Structure

```
bungewatch/
├── dags/
│   └── bungewatch_pipeline_dag.py   # Airflow DAG — 11 tasks, daily at 06:00 EAT
├── pipeline/
│   ├── config.py                    # Settings (pydantic-settings + .env)
│   ├── db.py                        # SQLAlchemy engine, upsert helpers
│   ├── logger.py                    # Structured logging + scrape_run context manager
│   ├── change_detector.py           # Stage transition detector
│   ├── pdf_downloader.py            # PDF fetch + retry
│   ├── pdf_parser.py                # Three-tier PDF text extractor (OCR capable)
│   ├── keyword_extractor.py         # YAKE + spaCy keyword pipeline
│   ├── claude_summarizer.py         # Extractive spaCy summarizer
│   └── foreign_law_matcher.py       # TF-IDF foreign law comparison
├── scrapers/
│   ├── kenyalaw_scraper.py          # KenyaLaw BS4 scraper
│   ├── parliament_scraper.py        # Parliament.go.ke Playwright scraper
│   └── selectors.py                 # CSS/XPath selector constants
├── dbt/
│   ├── models/
│   │   ├── staging/                 # stg_kenyalaw_bills, stg_parliament_bills
│   │   ├── intermediate/            # int_bills_unified, int_bill_stages
│   │   └── marts/                   # mart_active_bills, mart_bill_timeline,
│   │                                #   mart_keyword_frequency, mart_sponsor_stats
│   └── dbt_project.yml
├── streamlit_app/
│   ├── Home.py                      # Landing page
│   └── pages/
│       ├── 1_📋_Browse_Bills.py     # Full bill browser with filters
│       ├── 2_📄_Bill_Detail.py      # Per-bill deep dive (text, keywords, summary)
│       ├── 3_📊_Accountability.py   # Sponsor stats and stage analytics
│       └── 4_🔍_Search.py          # Full-text search across all bill text
├── seeds/                           # Foreign laws reference data
├── sql/                             # Raw schema DDL
├── docker-compose.yml               # postgres + airflow + streamlit
├── Dockerfile.airflow               # Airflow + Tesseract + Playwright + dbt
├── requirements.txt                 # Core Python deps
├── requirements.airflow.txt         # Airflow + NLP deps
└── .env.example                     # All required env vars documented
```

---

## Setup

### Prerequisites
- Docker Desktop (4 GB RAM recommended for OCR workload)
- Git

### Quick start

```bash
git clone https://github.com/declerke/BungeWatch.git
cd bungewatch
cp .env.example .env
# Edit .env — generate a Fernet key, set AIRFLOW_ADMIN_PASSWORD
docker compose up -d --build
```

On first boot the Airflow init container creates the database schema, then the scheduler picks up the DAG. Access:
- Airflow UI: http://localhost:8080 (admin / password from .env)
- Streamlit dashboard: http://localhost:8501

### Run the pipeline manually

```bash
# Trigger a full pipeline run via Airflow
docker exec bungewatch-airflow-scheduler-1 airflow dags trigger bungewatch_pipeline

# Or run individual pipeline functions directly
docker exec bungewatch-airflow-scheduler-1 python -c "
from scrapers.parliament_scraper import run; run()
"
```

### dbt only

```bash
docker exec bungewatch-airflow-scheduler-1 bash -c "
cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt
"
```

---

## dbt Models

| Model | Layer | Description |
|-------|-------|-------------|
| `stg_kenyalaw_bills` | Staging | Cleaned KenyaLaw bill records |
| `stg_parliament_bills` | Staging | Cleaned Parliament bill records |
| `int_bills_unified` | Intermediate | Deduplicated union of both sources |
| `int_bill_stages` | Intermediate | One row per stage transition |
| `mart_active_bills` | Mart | All current bills with latest stage + metadata |
| `mart_bill_timeline` | Mart | Stage progression timelines per bill |
| `mart_keyword_frequency` | Mart | Cross-bill keyword frequency rankings |
| `mart_sponsor_stats` | Mart | Bills per sponsor/party with stage counts |

All 28 dbt tests pass (not-null, unique, accepted-values, referential integrity).

---

## Dashboard

| Page | What it shows |
|------|--------------|
| Browse Bills | Searchable, filterable table of all 319 bills with source, chamber, stage |
| Bill Detail | Full extracted text, top keywords, plain-language summary, foreign law matches |
| Accountability | Sponsor league table, stage funnel, bills-per-session trends |
| Search | Full-text keyword search across all parsed bill text |

---

## Skills Demonstrated

- **Data engineering** — end-to-end pipeline from web scraping to analytics marts
- **Orchestration** — Airflow 3.0 DAG with task dependencies, retries, XCom state passing
- **Web scraping** — both static (BS4) and JS-rendered (Playwright) sources
- **Document processing** — multi-strategy PDF parsing with OCR fallback for scanned documents
- **NLP** — keyword extraction (YAKE), extractive summarisation (spaCy), text similarity (TF-IDF)
- **dbt** — layered transformation (staging → intermediate → marts), schema contracts, automated testing
- **Docker** — multi-service compose with custom Airflow image (Tesseract, Playwright, dbt pre-installed)
- **PostgreSQL** — schema design, upsert patterns, window functions in dbt
- **Civic tech** — applying data engineering to public accountability in a Kenyan context

---

## License

MIT
