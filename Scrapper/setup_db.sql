-- ============================================================
-- TENDER DAAS — Database Setup Script
-- ============================================================
-- Run this in TWO steps:
--
-- STEP A: Run these first 3 commands in psql as the postgres superuser:
--   psql -U postgres
--   Then paste the commands under "STEP A" below.
--
-- STEP B: Then connect to tender_db and run the rest:
--   psql -U tender_user -d tender_db -h localhost
--   Then paste everything under "STEP B" below.
-- ============================================================


-- ╔══════════════════════════════════════════════════╗
-- ║  STEP A: Run as postgres superuser              ║
-- ║  Command: psql -U postgres                      ║
-- ╚══════════════════════════════════════════════════╝

-- Create the user (change the password!)
CREATE USER tender_user WITH PASSWORD 'Nisha1@#$';

-- Create the database
CREATE DATABASE tender_db OWNER tender_user;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE tender_db TO tender_user;

-- NOW exit: \q
-- Then reconnect as tender_user (see Step B)


-- ╔══════════════════════════════════════════════════╗
-- ║  STEP B: Run as tender_user in tender_db        ║
-- ║  Command: psql -U tender_user -d tender_db -h localhost
-- ╚══════════════════════════════════════════════════╝

-- TABLE 1: tenders (main table)
CREATE TABLE IF NOT EXISTS tenders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Identification
    reference_number TEXT,
    title TEXT NOT NULL,
    title_clean TEXT,

    -- Organization
    organization TEXT NOT NULL,
    organization_short TEXT,
    department TEXT,

    -- Financial
    value BIGINT,
    value_display TEXT,
    emd_amount BIGINT,

    -- Dates
    date_published DATE,
    deadline TIMESTAMPTZ,
    bid_opening_date DATE,

    -- Classification
    category TEXT DEFAULT 'Uncategorized',
    subcategory TEXT,
    tender_type TEXT,
    procurement_type TEXT,

    -- Location
    state TEXT,
    district TEXT,

    -- Niche-specific metadata (flexible JSON)
    niche_metadata JSONB DEFAULT '{}',

    -- Documents
    document_urls TEXT[] DEFAULT '{}',
    document_count INTEGER DEFAULT 0,

    -- Source tracking
    source_portal TEXT NOT NULL,
    source_url TEXT,
    all_sources TEXT[] DEFAULT '{}',

    -- Status
    status TEXT DEFAULT 'open',

    -- Intelligence (filled by PDF processing later)
    boq_summary JSONB,
    eligibility JSONB,
    scope_summary TEXT,
    aoc_data JSONB,

    -- Pipeline tracking
    batch_id TEXT,
    hash TEXT,
    search_vector TSVECTOR,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);


-- TABLE 2: raw_records (safety net for re-processing)
CREATE TABLE IF NOT EXISTS raw_records (
    id SERIAL PRIMARY KEY,
    portal TEXT NOT NULL,
    raw_data JSONB NOT NULL,
    html_snapshot TEXT,
    batch_id TEXT,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    error_message TEXT
);


-- TABLE 3: tender_changes (tracks updates to tenders)
CREATE TABLE IF NOT EXISTS tender_changes (
    id SERIAL PRIMARY KEY,
    tender_id UUID REFERENCES tenders(id) ON DELETE CASCADE,
    change_type TEXT NOT NULL,
    field_name TEXT,
    old_value TEXT,
    new_value TEXT,
    detected_at TIMESTAMPTZ DEFAULT NOW()
);


-- TABLE 4: review_queue (potential duplicates)
CREATE TABLE IF NOT EXISTS review_queue (
    id SERIAL PRIMARY KEY,
    new_record_data JSONB NOT NULL,
    existing_tender_id UUID REFERENCES tenders(id),
    similarity_score FLOAT,
    reason TEXT,
    status TEXT DEFAULT 'pending',
    reviewed_at TIMESTAMPTZ
);


-- TABLE 5: scraper_runs (pipeline execution log)
CREATE TABLE IF NOT EXISTS scraper_runs (
    id SERIAL PRIMARY KEY,
    portal TEXT NOT NULL,
    batch_id TEXT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running',
    records_found INTEGER DEFAULT 0,
    records_new INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error_message TEXT
);


-- TABLE 6: niche_config (vertical definitions)
CREATE TABLE IF NOT EXISTS niche_config (
    id SERIAL PRIMARY KEY,
    niche_name TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    search_keywords TEXT[] NOT NULL,
    category_rules JSONB NOT NULL,
    metadata_schema JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================
-- INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_tenders_search ON tenders USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_tenders_deadline ON tenders(deadline);
CREATE INDEX IF NOT EXISTS idx_tenders_category ON tenders(category);
CREATE INDEX IF NOT EXISTS idx_tenders_state ON tenders(state);
CREATE INDEX IF NOT EXISTS idx_tenders_org ON tenders(organization_short);
CREATE INDEX IF NOT EXISTS idx_tenders_status ON tenders(status);
CREATE INDEX IF NOT EXISTS idx_tenders_ref ON tenders(reference_number);
CREATE INDEX IF NOT EXISTS idx_tenders_batch ON tenders(batch_id);
CREATE INDEX IF NOT EXISTS idx_tenders_hash ON tenders(hash);
CREATE INDEX IF NOT EXISTS idx_tenders_cat_state_deadline ON tenders(category, state, deadline);
CREATE INDEX IF NOT EXISTS idx_tenders_niche_meta ON tenders USING GIN(niche_metadata);
CREATE INDEX IF NOT EXISTS idx_raw_unprocessed ON raw_records(processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_raw_batch ON raw_records(batch_id);
CREATE INDEX IF NOT EXISTS idx_scraper_runs_batch ON scraper_runs(batch_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tenders_unique_ref
    ON tenders (reference_number, organization_short)
    WHERE reference_number IS NOT NULL;
-- ============================================
-- AUTO-UPDATE FUNCTIONS
-- ============================================

-- Auto-update search_vector
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.organization, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.state, '')), 'C') ||
        setweight(to_tsvector('english', COALESCE(NEW.category, '')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tsvector_update ON tenders;
CREATE TRIGGER tsvector_update
    BEFORE INSERT OR UPDATE ON tenders
    FOR EACH ROW EXECUTE FUNCTION update_search_vector();


-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_updated_at ON tenders;
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON tenders
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();


-- ============================================
-- INITIAL NICHE CONFIG (Solar + BESS)
-- ============================================
INSERT INTO niche_config (niche_name, display_name, search_keywords, category_rules)
VALUES (
    'solar_bess',
    'Solar + BESS',
    ARRAY['solar', 'solar PV', 'BESS', 'battery energy storage',
          'solar power plant', 'rooftop solar', 'solar hybrid',
          'energy storage system', 'solar module', 'solar EPC'],
    '{
        "rules": [
            {"keywords": ["solar", "bess"], "match": "all", "category": "Solar+BESS Hybrid"},
            {"keywords": ["solar", "battery"], "match": "all", "category": "Solar+BESS Hybrid"},
            {"keywords": ["bess", "energy storage"], "match": "any", "category": "BESS Only"},
            {"keywords": ["solar pv", "solar power", "rooftop solar", "solar epc"], "match": "any", "category": "Solar PV"}
        ]
    }'::jsonb
)
ON CONFLICT (niche_name) DO NOTHING;


-- ============================================
-- VERIFY
-- ============================================
\echo ''
\echo '=== Tables Created ==='
\dt

\echo ''
\echo '=== Setup Complete! ==='
