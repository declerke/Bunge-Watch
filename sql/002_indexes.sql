-- BungeWatch Kenya — Indexes
-- B-tree for filtered queries; GIN for full-text search

-- ─── bills ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_bills_current_stage    ON bills(current_stage);
CREATE INDEX IF NOT EXISTS idx_bills_sponsor          ON bills(sponsor);
CREATE INDEX IF NOT EXISTS idx_bills_date_introduced  ON bills(date_introduced DESC);
CREATE INDEX IF NOT EXISTS idx_bills_is_passed        ON bills(is_passed);
CREATE INDEX IF NOT EXISTS idx_bills_chamber          ON bills(chamber);
CREATE INDEX IF NOT EXISTS idx_bills_source           ON bills(source);
CREATE INDEX IF NOT EXISTS idx_bills_last_updated     ON bills(last_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_bills_foreign_checked  ON bills(foreign_match_checked_at) WHERE foreign_match_checked_at IS NULL;

-- Full-text search on bill title + sponsor
CREATE INDEX IF NOT EXISTS idx_bills_search ON bills USING GIN(search_vector);

-- ─── bill_stages ──────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_bill_stages_bill_id     ON bill_stages(bill_id);
CREATE INDEX IF NOT EXISTS idx_bill_stages_observed    ON bill_stages(bill_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_bill_stages_stage_name  ON bill_stages(stage_name);

-- ─── bill_keywords ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_bill_keywords_keyword ON bill_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_bill_keywords_bill_id ON bill_keywords(bill_id);

-- ─── bill_summaries ───────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_summaries_bill_lang ON bill_summaries(bill_id, language);
-- Full-text search on summary content
CREATE INDEX IF NOT EXISTS idx_summaries_search ON bill_summaries USING GIN(search_vector);

-- ─── bill_foreign_matches ─────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_foreign_matches_bill_id ON bill_foreign_matches(bill_id);
CREATE INDEX IF NOT EXISTS idx_foreign_matches_score   ON bill_foreign_matches(similarity_score DESC);

-- ─── raw_bill_pdfs ────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_raw_pdfs_bill_id ON raw_bill_pdfs(bill_id);
CREATE INDEX IF NOT EXISTS idx_raw_pdfs_status  ON raw_bill_pdfs(fetch_status);

-- ─── scrape_runs ──────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_scrape_runs_source ON scrape_runs(source, started_at DESC);

-- ─── claude_api_calls ─────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_claude_calls_bill_id ON claude_api_calls(bill_id);
CREATE INDEX IF NOT EXISTS idx_claude_calls_purpose ON claude_api_calls(purpose);
