# 🏛️ BungeWatch Kenya: Daily Legislative Intelligence Pipeline

**BungeWatch** is a production-grade civic data engineering pipeline that tracks every bill moving through Kenya's Parliament. It scrapes two official sources daily (KenyaLaw via BeautifulSoup4, Parliament.go.ke via Playwright), downloads the official PDFs, extracts text through a three-tier parser that cascades from pdfplumber to PyMuPDF to page-by-page Tesseract OCR for fully scanned documents, enriches each bill with YAKE keyword extraction and spaCy extractive summaries running entirely offline, compares bills against a 15-law foreign reference corpus using TF-IDF cosine similarity, transforms everything through an 8-model dbt pipeline, and surfaces the results in a 4-page Streamlit dashboard — making Kenya's legislative data searchable, understandable, and comparable to global legislation for the first time.

| Metric | Value |
|--------|-------|
| Bills tracked | 319 |
| PDFs downloaded | 319 (100%) |
| Bills parsed (text extracted) | 223 |
| Keywords extracted | 2,230 rows (10 per bill) |
| Plain-language summaries | 223 |
| Foreign law comparisons | 14 matches across 8 bills |
| dbt models / tests | 8 models · 28 tests (all passing) |
| Airflow DAG tasks | 11 |
| Dashboard pages | 4 |

---

## 🎯 Project Goal

Kenya's Parliament publishes bills across two separate portals — KenyaLaw and Parliament.go.ke — each with different data formats, coverage periods, and no cross-referencing. The vast majority of Parliament bills are scanned image PDFs with no embedded text, no search capability, and no plain-language explanation of what the bill actually does. Researchers and citizens wanting to understand pending legislation must download individual PDFs, read dense legal text, and have no way to know whether a provision has been adapted from legislation in another country.

BungeWatch automates the full intelligence chain: a daily Airflow DAG scrapes both sources, downloads every PDF, extracts text using whichever parser works (digital text-layer bills take under a second; 200-page scanned budget documents take 20+ minutes of parallel OCR), generates 2–5 sentence plain-English summaries and top-10 keyword tags offline, and runs TF-IDF similarity against a curated corpus of 15 reference laws from Uganda, Tanzania, South Africa, the UK, India, and the EU — flagging bills that appear to have borrowed provisions from foreign legislation. The resulting dataset feeds a Streamlit dashboard where any citizen can browse, search, and understand what Parliament is doing before it becomes law.

---

## 🧬 System Architecture

1. **Scraping — dual source** — `kenyalaw_scraper.py` (BeautifulSoup4, paginated static HTML) and `parliament_scraper.py` (Playwright Chromium, Drupal Views AJAX rendering) write raw bill records to PostgreSQL via idempotent `ON CONFLICT` upserts; `change_detector.py` records stage transitions (e.g. First Reading → Committee Stage) with timestamps

2. **PDF download** — `pdf_downloader.py` fetches each bill's official PDF with retry and exponential backoff; files are stored locally at `/data/pdfs/` using a SHA-256 content hash to detect re-uploads without re-downloading unchanged documents

3. **Three-tier PDF parsing** — `pdf_parser.py` cascades through three strategies per bill: pdfplumber (fast text-layer extraction for digital PDFs) → PyMuPDF (complex layout fallback) → Tesseract OCR (page-by-page at 150 DPI via `ThreadPoolExecutor(max_workers=4)` for scanned image PDFs); a `diagnose_pdf` function logs page count, encryption status, and image density when all three parsers fail

4. **Keyword extraction** — `keyword_extractor.py` runs YAKE (Yet Another Keyword Extractor) on each bill's text, normalises scores (YAKE produces lower = more relevant; scores are inverted to 0–1), then filters out proper nouns and legislative boilerplate using spaCy NER, storing the top 10 keywords per bill

5. **Extractive summarisation** — `claude_summarizer.py` uses spaCy `en_core_web_sm` to sentence-rank bill text by TF-IDF relevance, skipping the first 1,000 characters (cover page noise), filtering lines that are mostly uppercase headers or OCR fragments, and selecting the top 3–5 sentences as the plain-language summary — zero API cost, fully offline

6. **Foreign law comparison** — `foreign_law_matcher.py` builds a TF-IDF term-frequency vector for each bill (title + top keywords + first 4,000 characters of body text) and computes cosine similarity against pre-seeded vectors for 15 reference laws; matches above a score of 20 are stored with a generated explanation listing the shared conceptual terms; a `foreign_match_checked_at` timestamp prevents re-scanning bills on every DAG run

7. **dbt transformation** — `dbt run` materialises 8 models across three layers: staging (two source-specific views with type casting and deduplication) → intermediate (`int_bills_unified` deduplicates across sources, `int_bill_stages` unpivots stage history) → 4 mart tables (active bills with full enrichment, stage timelines, keyword frequency rankings, sponsor stats); `dbt test` runs 28 tests covering not-null, unique, accepted-values, and referential integrity

8. **Streamlit dashboard** — 4-page frontend reading exclusively from dbt mart views: Home (hero KPIs + recent stage changes), Browse Bills (filterable card table with foreign-inspired filter), Bill Detail (URL deep-linking via `?bill_id=`, legislative journey, keywords, summary, foreign law comparison panel), Accountability (pipeline overview charts for all 319 bills, foreign-inspired bills with similarity scores, sponsor leaderboard with pass rates), Search (PostgreSQL tsvector full-text search across all parsed bill text and summaries)

All 11 stages run as an **Apache Airflow 3.0 DAG** on a daily schedule (06:00 Africa/Nairobi) with XCom result passing, per-task retries, and exponential backoff.

---

## 🛠️ Technical Stack

| **Layer** | **Tool** | **Version** |
|---|---|---|
| Orchestration | Apache Airflow (LocalExecutor) | 3.0 |
| Scraping — static | BeautifulSoup4 + requests | 4.12 |
| Scraping — JS-rendered | Playwright (Chromium headless) | 1.47 |
| PDF parsing — text-layer | pdfplumber | 0.11 |
| PDF parsing — edge cases | PyMuPDF (fitz) | 1.24 |
| PDF parsing — scanned | Tesseract OCR + pdf2image | 5.3 |
| NLP — keywords | YAKE | 0.4.8 |
| NLP — NER + summarisation | spaCy (`en_core_web_sm`) | 3.7 |
| Text similarity | scikit-learn TF-IDF | 1.4 |
| Data storage | PostgreSQL | 15 |
| Data transformation | dbt-core + dbt-postgres | 1.7 |
| Dashboard | Streamlit | 1.40 |
| Containerisation | Docker Compose | 3 services |
| Language | Python | 3.11 |

---

## 📊 Performance & Results

- **319 bills** tracked across 2 official sources: KenyaLaw (43 bills — National Assembly + Senate with full sponsor/gazette metadata) and Parliament.go.ke (276 bills — 13th Parliament, 2022–present)
- **319/319 PDFs** downloaded — 100% success rate; SHA-256 content hashing prevents duplicate storage when Parliament re-publishes the same bill under a new URL
- **223 bills parsed** — 43 KenyaLaw bills parse instantly via text-layer; the remaining 180 Parliament bills required Tesseract OCR; ~96 bills are confirmed scanned-only with no recoverable text (blank pages, pre-1990 gazette scans)
- **Page-by-page OCR at 150 DPI** across 4 parallel ThreadPoolExecutor workers — peak RAM stays below 200 MB per worker regardless of bill size; a 23 MB, 200-page scanned budget document processes in ~30 minutes without OOM
- **2,230 YAKE keywords** extracted across 223 bills (10 per bill) with spaCy NER filtering to suppress proper-noun and boilerplate noise
- **223 plain-language summaries** generated fully offline — zero API calls, zero cost; average summary is 3–4 sentences extracting the bill's core regulatory intent
- **14 foreign law matches** across 8 bills against a 15-law seed corpus: Digital Health Bill (Ghana Data Protection Act + India DPDPA + Brazil LGPD), Gold Processing Bill (South Africa POPIA), Competition Amendment Bill (EU Digital Services Act), Computer Misuse Amendment Bill (UK Computer Misuse Act)
- **8 dbt models · 28 tests** — all passing; `mart_active_bills` contains 303 rows (319 bills minus 16 already-assented); `mart_sponsor_stats` covers 25 unique sponsors from KenyaLaw-sourced bills
- **11-task Airflow DAG** — tasks: `scrape_kenyalaw` → `scrape_parliament` → `detect_changes` → `download_pdfs` → `parse_pdfs` → `extract_keywords` → `generate_summaries` → `compare_foreign_laws` → `run_dbt` → `test_dbt` → `log_summary`

---

## 📸 Dashboard

### Home — Legislative Overview

![Home page](assets/home.png)

*Hero metrics: total bills tracked, parsed, and enriched. Daily pipeline status and recent stage changes feed.*

### Browse Bills — Full Bill Browser

![Browse Bills](assets/browse_bills.png)

*Filter by stage, chamber, year, and keyword tag. Toggle "Foreign-inspired only" to surface the 8 bills with foreign law similarity matches. Click Detail → to URL deep-link to a specific bill.*

### Bill Detail — Per-Bill Intelligence

![Bill Detail](assets/bill_detail.png)

*Full legislative journey tracker, top 10 YAKE keyword tags, 3–5 sentence spaCy extractive summary, and foreign law comparison panel with similarity scores and shared concept terms.*

### Accountability — Bill Pipeline Overview

![Accountability](assets/accountability.png)

*Stage distribution and source/chamber breakdown across all 319 bills — the complete legislative pipeline view.*

### Accountability — Foreign-Inspired Bills

![Foreign-Inspired Bills](assets/foreign_inspired_accountability.png)

*All 8 bills with TF-IDF cosine similarity ≥ 20 against the 15-law foreign reference corpus, with matched jurisdictions, similarity scores, and direct Detail links.*

### Accountability — Sponsor Leaderboard

![Sponsor Leaderboard](assets/sponsor_leaderboard_accountability.png)

*Sponsor pass-rate league table and bills-passed vs bills-pending breakdown (covers KenyaLaw-sourced bills, the only source that publishes sponsor names on the bill listing page).*

### Search — Full-Text Search

![Search](assets/search.png)

*PostgreSQL tsvector full-text search across all parsed bill text and summaries, ranked by `ts_rank` relevance.*

---

## 📑 Data Sources

| Source | Method | Bills | Metadata Available |
|--------|--------|-------|--------------------|
| [KenyaLaw](https://kenyalaw.org) | BeautifulSoup4 — paginated static HTML | 43 | Title, sponsor, gazette no., date introduced, chamber, stage, PDF URL |
| [Parliament of Kenya](https://parliament.go.ke) | Playwright — Drupal Views AJAX rendering | 276 | Title, chamber, stage, PDF URL (sponsor/gazette not published on listing page) |
| Foreign laws corpus | Seeded CSV (15 reference laws) | — | Jurisdiction, law name, year, summary, full-text URL |

---

## 🧠 Key Design Decisions

- **Three-tier PDF parser with graceful fallback** — Kenya's Parliament publishes both text-layer PDFs (newer bills) and fully scanned image PDFs (older bills and all budget documents). pdfplumber alone silently returns empty strings for ~80% of Parliament bills. The cascade (pdfplumber → PyMuPDF → Tesseract) maximises text recovery. When all three parsers fail, the `diagnose_pdf` function logs PDF internals — page count, encryption flag, image-to-text ratio — to distinguish genuinely blank bills from parser failures.

- **Page-by-page OCR at 150 DPI instead of bulk conversion** — loading a 300-page scanned bill at 200 DPI into memory requires ~2 GB RAM per worker and caused OOM kills during development. Calling `convert_from_path(first_page=N, last_page=N, dpi=150)` one page at a time keeps peak RSS below 200 MB per worker regardless of bill size, making the 4-worker configuration safe even on 8 GB machines.

- **4-worker ThreadPoolExecutor for parallel OCR** — Tesseract is CPU-bound but the Airflow task runs in a single Python process. `ThreadPoolExecutor(max_workers=4)` saturates available cores without spawning separate processes, avoiding inter-process coordination overhead and keeping Airflow's process model clean.

- **Fully offline NLP — zero API dependency** — summaries and keyword extraction use spaCy + YAKE, running entirely in-container. The pipeline operates indefinitely with zero marginal cost. `ANTHROPIC_API_KEY` is listed in `.env.example` as an optional future extension but the pipeline does not call any external API.

- **Playwright for Parliament.go.ke instead of direct HTTP** — the bills listing page is rendered by Drupal Views via AJAX; a plain `requests.get` returns an empty `<tbody>`. Playwright runs Chromium headless and waits for `networkidle` before reading the DOM, intercepting the fully-rendered table without reverse-engineering any private API endpoint.

- **dbt as sole source of truth for analytics tables** — all mart tables are produced by dbt with column-level documentation, source freshness checks, and automated tests. The Streamlit app never queries raw tables — only mart views. This means any data quality failure surfaces as a dbt test failure before the dashboard ever sees bad data.

- **`foreign_match_checked_at` stamp + partial index** — bills are stamped when their foreign law scan completes, regardless of whether matches were found. A partial GIN index on `foreign_match_checked_at IS NULL` keeps the scan queue O(new bills) rather than O(all bills), ensuring the DAG stays fast as the corpus grows.

- **`years` filter in `parse_all_downloaded()`** — the DAG targets only 2025/2026 Parliament bills on each OCR run, skipping the ~200 pre-2024 bills already confirmed as fully-scanned with no recoverable text. This reduces per-run OCR time from hours to minutes once the initial backfill is complete.

---

## 📂 Project Structure

```text
bungewatch/
├── dags/
│   └── bungewatch_pipeline_dag.py    # Airflow DAG — 11 tasks, daily at 06:00 EAT
├── pipeline/
│   ├── config.py                     # Pydantic Settings loaded from .env
│   ├── db.py                         # SQLAlchemy engine + idempotent upsert helpers
│   ├── logger.py                     # Structured logging + scrape_run context manager
│   ├── change_detector.py            # Stage transition detector (First → Committee, etc.)
│   ├── pdf_downloader.py             # PDF fetch with retry + SHA-256 dedup
│   ├── pdf_parser.py                 # Three-tier text extractor: pdfplumber → PyMuPDF → Tesseract
│   ├── keyword_extractor.py          # YAKE scoring + spaCy NER noise filter
│   ├── claude_summarizer.py          # spaCy extractive summariser (offline, no API)
│   └── foreign_law_matcher.py        # TF-IDF cosine similarity vs 15-law seed corpus
├── scrapers/
│   ├── kenyalaw_scraper.py           # BeautifulSoup4 scraper — paginated static HTML
│   ├── parliament_scraper.py         # Playwright scraper — Drupal Views AJAX rendering
│   └── selectors.py                  # CSS/XPath selector constants
├── dbt/
│   ├── models/
│   │   ├── staging/                  # stg_kenyalaw_bills, stg_parliament_bills (views)
│   │   ├── intermediate/             # int_bills_unified (dedup union), int_bill_stages (unpivoted)
│   │   └── marts/                    # mart_active_bills, mart_bill_timeline,
│   │                                 #   mart_keyword_frequency, mart_sponsor_stats (tables)
│   ├── seeds/                        # foreign_laws.csv — 15 reference laws with summaries
│   └── dbt_project.yml
├── streamlit_app/
│   ├── Home.py                       # Hero KPIs + recent stage changes + featured bills
│   ├── components/
│   │   ├── footer.py                 # Shared footer with NLP attribution
│   │   └── stage_progress.py         # Legislative journey progress bar component
│   ├── utils/
│   │   ├── db.py                     # Cached SQLAlchemy queries for all pages
│   │   └── formatting.py             # Stage badges, chamber labels, date formatting
│   └── pages/
│       ├── 1_📋_Browse_Bills.py      # Filterable bill cards with foreign-inspired toggle
│       ├── 2_📄_Bill_Detail.py       # URL deep-link bill view with full enrichment panel
│       ├── 3_📊_Accountability.py    # Pipeline overview + foreign matches + sponsor stats
│       └── 4_🔍_Search.py           # tsvector full-text search with ts_rank scoring
├── sql/
│   ├── 001_schema.sql                # 14-table schema (Bronze → Silver → Gold → Ops layers)
│   └── 002_indexes.sql               # Performance indexes + partial GIN on foreign_match_checked_at IS NULL
├── tests/
│   └── test_change_detector.py       # Unit tests for stage transition detection logic
├── assets/                           # Dashboard screenshots
├── docker-compose.yml                # postgres + airflow-scheduler + streamlit (3 services)
├── Dockerfile.airflow                # Airflow image with Tesseract, Playwright, dbt, spaCy pre-installed
├── requirements.txt                  # Core Python dependencies
├── requirements.airflow.txt          # Airflow + NLP deps (spaCy, YAKE, pdfplumber, Tesseract bindings)
└── .env.example                      # All required environment variables with documentation
```

---

## ⚙️ Installation & Setup

### Prerequisites

- Docker Desktop (4 GB RAM minimum — Tesseract OCR is memory-intensive for large scanned PDFs)
- Git

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/declerke/Bunge-Watch.git
   cd Bunge-Watch
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env — generate a Fernet key for Airflow and set AIRFLOW_ADMIN_PASSWORD
   # ANTHROPIC_API_KEY is optional — the pipeline runs fully without it
   ```

3. **Build and start all services**
   ```bash
   docker compose up -d --build
   ```
   First build downloads spaCy `en_core_web_sm` (~43 MB) and installs Tesseract + Playwright Chromium inside the Airflow image.

4. **Wait for initialisation** (~2–3 minutes on first run)
   ```bash
   docker compose logs -f airflow-scheduler
   # Wait until: "Scheduler started"
   ```

5. **Trigger the pipeline**
   - Open Airflow at `http://localhost:8080`
   - Enable and trigger the `bungewatch_pipeline` DAG
   - First run downloads all PDFs and OCRs scanned bills — allow 2–4 hours for full backfill

6. **Access the stack**

   | Service | URL |
   |---------|-----|
   | Streamlit dashboard | http://localhost:8501 |
   | Airflow UI | http://localhost:8080 |

### Run individual pipeline stages

```bash
# Parse only 2025/2026 Parliament bills (OCR-heavy stage)
docker exec bungewatch-airflow-scheduler-1 python3 -c "
import sys; sys.path.insert(0, '/opt/airflow')
from pipeline.pdf_parser import parse_all_downloaded
print(parse_all_downloaded(years=[2025, 2026]))
"

# Run dbt models and tests
docker exec bungewatch-airflow-scheduler-1 bash -c "
  cd /opt/airflow/dbt && \
  dbt run --profiles-dir /opt/airflow/dbt && \
  dbt test --profiles-dir /opt/airflow/dbt
"
```

---

## 🗄️ dbt Models

| Model | Layer | Type | Description |
|-------|-------|------|-------------|
| `stg_kenyalaw_bills` | Staging | View | Type-cast and normalised KenyaLaw records; filters out bills with no PDF URL |
| `stg_parliament_bills` | Staging | View | Type-cast Parliament records; coalesces null sponsor to `'Unknown'` |
| `int_bills_unified` | Intermediate | View | Deduplicates across both sources on `(bill_number, chamber, bill_year)`; resolves source priority conflicts |
| `int_bill_stages` | Intermediate | View | Unpivots stage history into one row per transition; computes `days_ago` from `stage_date` |
| `mart_active_bills` | Mart | Table | All 303 non-assented bills with latest stage, summary, keyword tags, and `foreign_match_count` (threshold ≥ 20) |
| `mart_bill_timeline` | Mart | Table | Stage progression timelines per bill — used by the Recent Changes feed and Detail page journey tracker |
| `mart_keyword_frequency` | Mart | Table | Cross-bill keyword frequency rankings for trending-topics analysis |
| `mart_sponsor_stats` | Mart | Table | 25 sponsors with `bills_introduced`, `bills_passed`, `pass_rate_pct`, and active year span |

**28 dbt tests — 28/28 PASS:**
- Staging: `unique` + `not_null` on `bill_id`; `accepted_values` on `chamber` (`NA`, `Senate`), `current_stage`
- Intermediate: `unique` on unified `bill_id`; referential integrity between `int_bill_stages` and `int_bills_unified`
- Marts: `not_null` on `bill_id`, `title`, `current_stage`; `unique` on `bill_id` in `mart_active_bills`; `not_null` on `sponsor` in `mart_sponsor_stats`

---

## 🎓 Skills Demonstrated

- **Dual-source web scraping with mismatched rendering strategies** — KenyaLaw serves paginated static HTML parsed with BeautifulSoup4; Parliament.go.ke renders its bills table via Drupal Views AJAX, requiring Playwright to wait for `networkidle` before reading the DOM. Both scrapers write to the same schema via idempotent `ON CONFLICT (bill_id) DO UPDATE` upserts

- **Three-tier document processing pipeline** — pdfplumber extracts text-layer PDFs in under a second; PyMuPDF handles complex column layouts that confuse pdfplumber; Tesseract OCR with `pdf2image` processes fully scanned bills page-by-page at 150 DPI; `diagnose_pdf` logs internal PDF metadata (page count, encryption, image density) to distinguish blank documents from parser failures

- **Production-safe parallel OCR** — `ThreadPoolExecutor(max_workers=4)` runs Tesseract workers concurrently within the Airflow task process; page-by-page `convert_from_path(first_page=N, last_page=N)` caps peak RSS at 200 MB per worker regardless of bill length, eliminating OOM kills on large scanned budget documents

- **Fully offline NLP pipeline** — YAKE keyword extraction produces language-agnostic statistical scores (lower = more relevant; inverted to 0–1 for storage); spaCy NER filters proper nouns and legislative boilerplate from keyword candidates; spaCy sentence ranking produces extractive 3–5 sentence summaries by TF-IDF weight with OCR noise rejection — no external API required at any stage

- **TF-IDF foreign law comparison with incremental scan queue** — each bill's keyword + body text is vectorised into a TF term-frequency dict; cosine similarity is computed against 15 pre-seeded foreign law vectors; a `foreign_match_checked_at` timestamp and a partial GIN index on `IS NULL` keep the daily scan queue O(new bills) rather than O(all bills)

- **Apache Airflow 3.0 DAG design** — 11-task DAG with parallel scraping branches gating on a shared `detect_changes` task, downstream NLP tasks with XCom result passing, per-task retry with exponential backoff, and a `years` parameter to the OCR task that limits re-processing to only 2025/2026 Parliament bills on incremental runs

- **dbt analytics engineering** — staging → intermediate → mart pattern across 8 models; staging views enforce type contracts and handle source-specific nulls; `int_bills_unified` deduplicates across sources by bill identity rather than by primary key; mart tables are the exclusive data contract between the pipeline and the dashboard

- **PostgreSQL advanced schema patterns** — 14-table schema across Bronze/Silver/Gold/Ops layers; `tsvector` full-text search index on a composite of bill title + body text + summary; partial GIN index on `foreign_match_checked_at IS NULL` for O(unscanned) queue queries; `ON CONFLICT DO UPDATE` upserts with SHA-256 content hashing to prevent duplicate PDF storage

- **Streamlit multi-page dashboard with URL deep-linking** — `st.query_params.get("bill_id")` enables shareable direct links to individual bill detail pages; `@st.cache_data(ttl=600)` caches all database queries; the Accountability page serves separate sections for the pipeline-wide stage breakdown (all 319 bills) and the sponsor leaderboard (KenyaLaw-only), with a clear data-coverage note for each

- **Civic technology applied to public accountability** — the entire system is designed around a real governance gap: Kenyan citizens have no searchable, understandable interface to their Parliament's legislative output; BungeWatch treats that as a data engineering problem and solves it end-to-end with open-source tooling and zero recurring cloud cost

---

## License

MIT
