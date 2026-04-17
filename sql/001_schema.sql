-- BungeWatch Kenya — Database Schema
-- 14 tables: Bronze (raw) | Silver (canonical) | Gold stubs | Ops | Phase-2 stubs

-- ─── Extensions ───────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ═══════════════════════════════════════════════════════════════════════════════
-- BRONZE — Raw ingested data (never mutated after insert)
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS raw_kenyalaw_scrapes (
    id           SERIAL PRIMARY KEY,
    scraped_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    year         INTEGER     NOT NULL,
    raw_html     TEXT        NOT NULL,
    record_count INTEGER
);

CREATE TABLE IF NOT EXISTS raw_parliament_scrapes (
    id          SERIAL PRIMARY KEY,
    scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    page_type   VARCHAR(20) NOT NULL,  -- 'bills' | 'tracker'
    raw_json    JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_bill_pdfs (
    id             SERIAL PRIMARY KEY,
    bill_id        VARCHAR(120) NOT NULL,
    pdf_url        TEXT         NOT NULL,
    pdf_sha256     VARCHAR(64),
    local_path     TEXT,
    fetched_at     TIMESTAMPTZ,
    fetch_status   VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending | success | failed
    error_message  TEXT,
    UNIQUE (bill_id, pdf_url)
);

CREATE TABLE IF NOT EXISTS raw_bill_text (
    id           SERIAL PRIMARY KEY,
    bill_id      VARCHAR(120) NOT NULL,
    full_text    TEXT,
    char_count   INTEGER,
    extracted_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    parser_used  VARCHAR(20),              -- pdfplumber | pymupdf
    parse_status VARCHAR(20)  NOT NULL DEFAULT 'success',  -- success | failed | empty
    UNIQUE (bill_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- SILVER — Canonical, deduplicated, source-reconciled
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bills (
    bill_id          VARCHAR(120) PRIMARY KEY,
    source           VARCHAR(20)  NOT NULL,        -- kenyalaw | parliament
    bill_number      VARCHAR(60),
    title            TEXT         NOT NULL,
    sponsor          TEXT,
    sponsor_party    TEXT,
    chamber          VARCHAR(10)  NOT NULL DEFAULT 'NA',  -- NA | Senate
    date_introduced  DATE,
    gazette_no       VARCHAR(60),
    current_stage    VARCHAR(60),
    is_passed        BOOLEAN      NOT NULL DEFAULT FALSE,
    assent_date      DATE,
    source_url       TEXT,
    pdf_url          TEXT,
    text_sha256                VARCHAR(64),
    search_vector              TSVECTOR,
    foreign_match_checked_at   TIMESTAMPTZ,
    first_seen_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bill_stages (
    id           SERIAL PRIMARY KEY,
    bill_id      VARCHAR(120) NOT NULL REFERENCES bills(bill_id) ON DELETE CASCADE,
    stage_name   VARCHAR(60)  NOT NULL,
    stage_date   DATE,
    source       VARCHAR(20),
    observed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bill_keywords (
    id               SERIAL PRIMARY KEY,
    bill_id          VARCHAR(120) NOT NULL REFERENCES bills(bill_id) ON DELETE CASCADE,
    keyword          VARCHAR(120) NOT NULL,
    relevance_score  FLOAT,
    UNIQUE (bill_id, keyword)
);

CREATE TABLE IF NOT EXISTS bill_summaries (
    id               SERIAL PRIMARY KEY,
    bill_id          VARCHAR(120) NOT NULL REFERENCES bills(bill_id) ON DELETE CASCADE,
    language         VARCHAR(5)   NOT NULL DEFAULT 'en',  -- en | sw
    summary_short    TEXT,
    summary_detailed TEXT,
    model_version    VARCHAR(60),
    input_sha256     VARCHAR(64),
    search_vector    TSVECTOR,
    generated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (bill_id, language)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- FOREIGN LAW COMPARISON — Core differentiator
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS foreign_laws (
    id            SERIAL PRIMARY KEY,
    jurisdiction  VARCHAR(100) NOT NULL,
    law_name      TEXT         NOT NULL,
    law_year      INTEGER,
    summary       TEXT,
    full_text_url TEXT,
    keywords      TEXT[],
    seeded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (jurisdiction, law_name)
);

CREATE TABLE IF NOT EXISTS bill_foreign_matches (
    id               SERIAL PRIMARY KEY,
    bill_id          VARCHAR(120) NOT NULL REFERENCES bills(bill_id) ON DELETE CASCADE,
    foreign_law_id   INTEGER      NOT NULL REFERENCES foreign_laws(id) ON DELETE CASCADE,
    similarity_score FLOAT,
    explanation      TEXT,
    generated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (bill_id, foreign_law_id)
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- OPS — Pipeline transparency & cost audit
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS scrape_runs (
    id               SERIAL PRIMARY KEY,
    source           VARCHAR(60)  NOT NULL,
    started_at       TIMESTAMPTZ  NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(20)  NOT NULL DEFAULT 'running',  -- running | success | failed
    records_ingested INTEGER      NOT NULL DEFAULT 0,
    records_updated  INTEGER      NOT NULL DEFAULT 0,
    error_message    TEXT
);

CREATE TABLE IF NOT EXISTS claude_api_calls (
    id            SERIAL PRIMARY KEY,
    bill_id       VARCHAR(120),
    purpose       VARCHAR(50),    -- summary_en | foreign_match
    model         VARCHAR(60),
    input_tokens  INTEGER,
    output_tokens INTEGER,
    cost_usd      NUMERIC(10, 6),
    called_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════════════════════════
-- PHASE 2 STUBS — Schema-ready, empty, no UI yet
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS subscriptions (
    id                SERIAL PRIMARY KEY,
    email             VARCHAR(255) NOT NULL,
    keyword           VARCHAR(100),
    confirmed         BOOLEAN      NOT NULL DEFAULT FALSE,
    unsubscribe_token VARCHAR(64)  UNIQUE,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hansard_mentions (
    id           SERIAL PRIMARY KEY,
    bill_id      VARCHAR(120) REFERENCES bills(bill_id) ON DELETE SET NULL,
    mp_name      VARCHAR(200),
    excerpt      TEXT,
    sitting_date DATE,
    hansard_url  TEXT
);

CREATE TABLE IF NOT EXISTS county_bills (
    bill_id       VARCHAR(120) PRIMARY KEY,
    county_name   VARCHAR(100),
    title         TEXT         NOT NULL,
    sponsor       TEXT,
    current_stage VARCHAR(60),
    source_url    TEXT,
    first_seen_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
