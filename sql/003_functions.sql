-- BungeWatch Kenya — Functions & Triggers

-- ─── Auto-update last_updated_at on bills ────────────────────────────────────
CREATE OR REPLACE FUNCTION update_last_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_bills_updated_at
    BEFORE UPDATE ON bills
    FOR EACH ROW
    EXECUTE FUNCTION update_last_updated_at();

-- ─── Maintain bills.search_vector (title A, sponsor B) ───────────────────────
CREATE OR REPLACE FUNCTION update_bill_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector =
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.sponsor, '')), 'B');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_bills_search_vector
    BEFORE INSERT OR UPDATE OF title, sponsor ON bills
    FOR EACH ROW
    EXECUTE FUNCTION update_bill_search_vector();

-- ─── Maintain bill_summaries.search_vector ────────────────────────────────────
CREATE OR REPLACE FUNCTION update_summary_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector = to_tsvector(
        'english',
        COALESCE(NEW.summary_short, '') || ' ' || COALESCE(NEW.summary_detailed, '')
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_summary_search_vector
    BEFORE INSERT OR UPDATE OF summary_short, summary_detailed ON bill_summaries
    FOR EACH ROW
    EXECUTE FUNCTION update_summary_search_vector();
