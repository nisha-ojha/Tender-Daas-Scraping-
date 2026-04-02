-- ============================================================
-- TENDER DAAS — Normalized Schema v3 (Corrected)
-- ============================================================
--
-- DESIGN RULES:
--   1. tenders.id  (UUID) is the single source of truth
--   2. All child tables reference tenders.id as foreign key
--   3. portal lives ONLY in tenders — no repetition in child tables
--   4. reference_number kept in child tables for fast querying
--      without always needing a JOIN
--
-- TABLE MAP:
--   tenders              → core identity + portal + description
--   tender_details       → dates, status, location, notification
--   tender_financial     → EMD, SD, PG, fees, tariff
--   tender_documents     → PDFs linked to notification number
--   tender_technical     → capacity, eligibility, bid system
--   tender_contacts      → contact persons and offices
--   tender_bidders       → L1 / L2 / L3 bidder data
--   tender_awards        → winning contractor info
--   raw_records          → scraper safety net
--   scraper_runs         → pipeline execution log
--   niche_config         → sector/vertical definitions
--   companies            → contractor entity registry
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";


-- ============================================================
-- TABLE 1: tenders
-- Core identity. Every other table references this via tender_id.
-- portal lives here — child tables inherit it via JOIN.
-- ============================================================
CREATE TABLE IF NOT EXISTS tenders (
    -- ── Primary Key ───────────────────────────────────────
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- ── Portal (lives here, not repeated in child tables) ─
    portal              TEXT NOT NULL,
    -- seci / cppp / gem / ntpc / mnre / state_portal

    -- ── Identity ──────────────────────────────────────────
    reference_number    TEXT,
    -- e.g. SECI/C&P/IPP/13/0020/25-26
    -- Unique per portal — enforced by unique index below

    cppp_tender_id      TEXT,
    -- Cross-portal deduplication key
    -- SECI stores the CPPP equivalent ID on their detail page

    -- ── Title & Description ───────────────────────────────
    title               TEXT NOT NULL,
    title_clean         TEXT,
    -- Normalised title for fuzzy dedup matching
    -- Lowercased, stopwords removed, punctuation stripped

    description         TEXT,
    -- Full tender description / scope summary
    -- Sourced from detail page or first page of RfS PDF
    -- Used for full-text search and LLM summarisation

    -- ── Organisation ──────────────────────────────────────
    organization        TEXT NOT NULL,
    -- Full name: "Solar Energy Corporation of India"
    organization_short  TEXT,
    -- Abbreviation: "SECI"
    department          TEXT,
    -- Sub-department if any: "Contract & Procurement Division"
    ministry            TEXT,
    -- "Ministry of New & Renewable Energy"

    -- ── Classification ────────────────────────────────────
    category            TEXT DEFAULT 'Uncategorized',
    -- Solar PV / Wind / BESS Only / Solar+BESS Hybrid /
    -- Hybrid RE / Green Hydrogen / O&M / EPC / Consultancy /
    -- Equipment Supply / Rooftop / Transmission / IT / Other
    subcategory         TEXT,
    tender_type         TEXT,
    -- Open / Limited / Single Source / EOI / RfS / RfP / RfQ / NIT
    procurement_type    TEXT,
    -- Works / Goods / Services / Consulting

    -- ── Source URL ────────────────────────────────────────
    source_url          TEXT,
    -- The listing page URL where this tender was found
    detail_url          TEXT,
    -- The tender detail page URL
    all_sources         TEXT[],
    -- If same tender appears on multiple portals

    -- ── Pipeline Tracking ─────────────────────────────────
    batch_id            TEXT,
    -- run_20260326_183149_seci — links to scraper_runs
    hash                TEXT,
    -- SHA-256 of portal|reference_number|title_clean
    -- Used to detect content changes between runs

    -- ── Search ────────────────────────────────────────────
    search_vector       TSVECTOR,
    -- Auto-updated by trigger
    -- Weighted: title(A) > org(B) > description(C) > category(C)

    -- ── Timestamps ────────────────────────────────────────
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Every tender is unique per portal
CREATE UNIQUE INDEX IF NOT EXISTS idx_tenders_unique_ref
    ON tenders (reference_number, portal)
    WHERE reference_number IS NOT NULL;


-- ============================================================
-- TABLE 2: tender_details
-- Dates, status, location, notification details.
-- One-to-one with tenders.
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_details (
    id                      SERIAL PRIMARY KEY,
    tender_id               UUID UNIQUE NOT NULL
                                REFERENCES tenders(id) ON DELETE CASCADE,
    -- Note: portal and reference_number are in tenders
    -- Use JOIN to get them. reference_number kept here for fast queries.
    reference_number        TEXT,

    -- ── Status ────────────────────────────────────────────
    status                  TEXT DEFAULT 'open',
    -- open / closed / awarded / cancelled / suspended
    -- open    = live tenders (accepting bids)
    -- closed  = archived (bidding period over)
    -- awarded = result published, winner known
    -- cancelled = tender withdrawn by issuer

    -- ── Key Dates ─────────────────────────────────────────
    date_published          DATE,
    -- When the tender was published on the portal

    pre_bid_date            TIMESTAMPTZ,
    -- Pre-bid meeting date (important for large tenders)

    bid_submission_online   TIMESTAMPTZ,
    -- Online portal submission deadline

    bid_submission_offline  TIMESTAMPTZ,
    -- Physical document submission deadline (if applicable)

    deadline                TIMESTAMPTZ,
    -- Primary deadline = bid_submission_online usually

    bid_opening_date        TIMESTAMPTZ,
    -- When bids are opened (Technical bid for 2-bid system)

    financial_bid_opening   TIMESTAMPTZ,
    -- Only for 2-bid system: when price bids are opened

    award_date              DATE,
    -- Date LoA was issued

    ppa_signing_date        DATE,
    -- Power Purchase Agreement signing date

    cod_date                DATE,
    -- Commercial Operation Date (project completion target)

    -- ── Notification ──────────────────────────────────────
    notification_number     TEXT,
    -- NIT No. / Tender Notice No. as shown on documents
    -- This links tenders to their PDF documents

    notification_date       DATE,

    corrigendum_count       INTEGER DEFAULT 0,
    -- Number of amendments/corrigenda issued so far

    -- ── Location ──────────────────────────────────────────
    state                   TEXT,
    district                TEXT,
    region                  TEXT,
    -- North / South / East / West / Central / Pan India
    location_text           TEXT,
    -- Raw "Place of Work" field from portal
    country                 TEXT DEFAULT 'India',

    -- ── Portal-specific extras ────────────────────────────
    extra_data              JSONB DEFAULT '{}',
    -- Catch-all for fields unique to one portal
    -- e.g. {"seci_tender_id": "1020", "gem_bid_id": "xyz"}

    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 3: tender_financial
-- All money-related fields for a tender.
-- One-to-one with tenders.
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_financial (
    id                      SERIAL PRIMARY KEY,
    tender_id               UUID UNIQUE NOT NULL
                                REFERENCES tenders(id) ON DELETE CASCADE,
    reference_number        TEXT,

    -- ── Project Value ─────────────────────────────────────
    estimated_value         BIGINT,
    -- Total project cost estimate in ₹ (Rupees)
    estimated_value_display TEXT,
    -- Human readable: "₹142.50 Crore"
    value_is_estimated      BOOLEAN DEFAULT TRUE,
    -- FALSE when actual value is confirmed (post-award)

    -- ── EMD (Earnest Money Deposit) ───────────────────────
    emd_amount              BIGINT,
    -- In ₹. Forfeited if bidder withdraws after submission.
    emd_raw_text            TEXT,
    -- Verbatim from portal: "1% of project cost" or "₹59,000"
    emd_per_mw              BIGINT,
    -- If EMD is formula-based: ₹ per MW of project capacity
    emd_currency            TEXT DEFAULT 'INR',
    emd_is_formula          BOOLEAN DEFAULT FALSE,
    -- TRUE when EMD = "X% of project cost" (not a fixed amount)
    emd_exemption_msme      BOOLEAN DEFAULT FALSE,
    emd_exemption_startup   BOOLEAN DEFAULT FALSE,

    -- ── Tender / Document Fee ─────────────────────────────
    tender_fee              BIGINT,
    -- Cost to purchase tender documents. Non-refundable.
    tender_fee_raw          TEXT,
    -- Verbatim: "₹10,000 + 18% GST"
    tender_fee_refundable   BOOLEAN DEFAULT FALSE,

    -- ── Processing Fee ────────────────────────────────────
    processing_fee          BIGINT,
    processing_fee_per_mw   BIGINT,
    -- Some SECI tenders charge per MW of capacity bid
    processing_fee_raw      TEXT,

    -- ── Security Deposit (SD) ─────────────────────────────
    sd_percentage           NUMERIC(5,2),
    -- % of contract value. Returned after contract completion.
    sd_raw_text             TEXT,
    sd_form                 TEXT,
    -- BG (Bank Guarantee) / DD (Demand Draft) / Cash

    -- ── Performance Guarantee (PG) ────────────────────────
    pg_percentage           NUMERIC(5,2),
    -- % of contract value. Held during O&M/warranty period.
    pg_raw_text             TEXT,
    pg_validity_months      INTEGER,
    pg_form                 TEXT,

    -- ── Liquidated Damages (LD) ───────────────────────────
    liquidated_damages_pct  NUMERIC(5,2),
    -- % per week of delay. Deducted from payments.
    ld_cap_pct              NUMERIC(5,2),
    -- Maximum LD cap (usually 10% of contract value)

    -- ── Tariff ────────────────────────────────────────────
    tariff_ceiling          NUMERIC(8,4),
    -- ₹/unit. Maximum tariff SECI will accept. Critical field.
    tariff_floor            NUMERIC(8,4),
    l1_tariff               NUMERIC(8,4),
    -- Winning tariff after award. Populated from LoA PDF.
    tariff_type             TEXT,
    -- fixed / VGF / FiT / hybrid / reverse-auction

    -- ── Payment Terms ─────────────────────────────────────
    payment_security        TEXT,
    -- Letter of Credit / Bank Guarantee / Escrow
    mobilisation_advance    BOOLEAN DEFAULT FALSE,
    advance_percentage      NUMERIC(5,2),

    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 4: tender_documents
-- All PDFs and attachments for a tender.
-- One-to-many with tenders (multiple documents per tender).
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_documents (
    id                  SERIAL PRIMARY KEY,
    tender_id           UUID NOT NULL
                            REFERENCES tenders(id) ON DELETE CASCADE,
    reference_number    TEXT,

    -- ── Notification Link ─────────────────────────────────
    notification_number TEXT,
    -- Links this document to the NIT/notification it belongs to
    -- Important: corrigenda have their own notification numbers

    -- ── Document Identity ─────────────────────────────────
    doc_name            TEXT,
    -- Filename as shown on portal: "RfS_for_1200MW_Solar.pdf"
    doc_url             TEXT NOT NULL,
    -- Full download URL

    doc_type            TEXT,
    -- RfS / RfP / NIT / BOQ / BidOpening / LoA / Corrigendum /
    -- PPA / PSA / PreBid / IntegrityPact / Drawing / Other

    uploaded_date       DATE,
    file_size_kb        INTEGER,

    is_amendment        BOOLEAN DEFAULT FALSE,
    -- TRUE for corrigenda/amendments
    amendment_number    INTEGER,
    -- 1, 2, 3... for tracking amendment sequence

    -- ── Download Status ───────────────────────────────────
    downloaded          BOOLEAN DEFAULT FALSE,
    downloaded_at       TIMESTAMPTZ,
    local_path          TEXT,
    -- storage/pdfs/seci/SECI_C_P_IPP_13_0020_25-26/RfS.pdf

    -- ── Parse Status ──────────────────────────────────────
    parsed              BOOLEAN DEFAULT FALSE,
    parsed_at           TIMESTAMPTZ,
    parse_error         TEXT,
    page_count          INTEGER,
    text_length         INTEGER,
    -- Character count of extracted text (0 = scanned/image PDF)

    extracted_data      JSONB,
    -- Structured data pulled from this specific document
    -- e.g. {"capacity_mw": 1200, "tariff_ceiling": 2.42}

    batch_id            TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 5: tender_technical
-- Technical scope, capacity, eligibility, bid system.
-- One-to-one with tenders.
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_technical (
    id                      SERIAL PRIMARY KEY,
    tender_id               UUID UNIQUE NOT NULL
                                REFERENCES tenders(id) ON DELETE CASCADE,
    reference_number        TEXT,

    -- ── Capacity ──────────────────────────────────────────
    capacity_mw             NUMERIC(10,2),
    -- MW of power generation capacity
    capacity_mwh            NUMERIC(10,2),
    -- MWh of storage capacity (BESS/PSP tenders)
    capacity_kw             NUMERIC(10,2),
    -- kW for smaller rooftop projects
    capacity_raw_text       TEXT,
    -- Verbatim: "1200 MW ISTS-Connected Solar PV"

    -- ── Power & Project Type ──────────────────────────────
    power_type              TEXT,
    -- Solar / Wind / RTC / Peak / Firm / Hybrid / Thermal
    energy_storage_required BOOLEAN,
    -- TRUE if BESS/ESS is mandatory component
    project_model           TEXT,
    -- BOO / BOOT / EPC / O&M / Turnkey / Rate Contract

    -- ── Connectivity & Location ───────────────────────────
    connectivity            TEXT,
    -- ISTS (inter-state) / STU (state) / Dedicated
    interconnection_point   TEXT,
    -- "400kV Fatehpur GSS, Rajasthan"
    substation_kv           INTEGER,
    -- Voltage level: 400 / 220 / 132 / 66
    land_acres              NUMERIC(10,2),
    land_responsibility     TEXT,
    -- Developer / Government / SECI / Hybrid

    -- ── Bid System ────────────────────────────────────────
    no_of_covers            INTEGER,
    -- 1 = Single bid, 2 = Two-bid (Tech + Financial)
    bid_system_type         TEXT,
    -- Single-Bid / Two-Bid / Three-Bid
    reverse_auction         BOOLEAN DEFAULT FALSE,
    -- TRUE if e-Reverse Auction follows bid submission
    domestic_content_req    BOOLEAN,
    -- TRUE if DCR (Domestic Content Requirement) applies
    is_international        BOOLEAN DEFAULT FALSE,
    -- TRUE if foreign bidders allowed

    -- ── Contract Duration ─────────────────────────────────
    contract_type           TEXT,
    -- EPC / O&M / Turnkey / Rate Contract / Framework
    om_period_years         INTEGER,
    -- O&M contract duration
    ppa_duration_years      INTEGER,
    -- Power Purchase Agreement tenure (typically 25 years)
    concession_period_years INTEGER,

    -- ── Eligibility ───────────────────────────────────────
    net_worth_required_cr   NUMERIC(10,2),
    -- Minimum net worth in Crore ₹
    turnover_required_cr    NUMERIC(10,2),
    -- Minimum annual turnover in Crore ₹
    experience_required_mw  NUMERIC(10,2),
    -- Prior commissioned capacity in MW
    experience_raw_text     TEXT,
    -- Verbatim eligibility text from RfS
    consortium_allowed      BOOLEAN,
    max_consortium_members  INTEGER,
    foreign_bidder_allowed  BOOLEAN,
    msme_exemption          BOOLEAN,
    startup_eligible        BOOLEAN,
    eligibility             JSONB,
    -- Full structured eligibility (for fields not in columns above)

    -- ── BOQ (Bill of Quantities) ──────────────────────────
    boq_items               JSONB,
    -- Parsed line items from BOQ PDF
    -- [{"sno":1,"description":"Solar Modules 540Wp",
    --   "unit":"Nos","quantity":250000,
    --   "unit_rate":12500,"total":3125000000}]
    boq_total_value         BIGINT,
    boq_extracted           BOOLEAN DEFAULT FALSE,

    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 6: tender_contacts
-- Contact persons and offices per tender.
-- One-to-many with tenders.
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_contacts (
    id                  SERIAL PRIMARY KEY,
    tender_id           UUID NOT NULL
                            REFERENCES tenders(id) ON DELETE CASCADE,
    reference_number    TEXT,

    -- ── Contact Type ──────────────────────────────────────
    contact_type        TEXT,
    -- TenderIssuing / NodalOfficer / BidSubmission /
    -- PreBidContact / TechnicalContact / FinancialContact

    -- ── Person Details ────────────────────────────────────
    name                TEXT,
    designation         TEXT,
    -- "Deputy Manager (C&P)"
    department          TEXT,
    organization        TEXT,

    -- ── Contact Reach ─────────────────────────────────────
    email               TEXT,
    phone               TEXT,
    mobile              TEXT,
    fax                 TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    pincode             TEXT,
    website             TEXT,

    -- ── Source ────────────────────────────────────────────
    source              TEXT,
    -- detail_page / pdf / manual
    created_at          TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 7: tender_bidders
-- L1 / L2 / L3 bidder data — from Bid Opening Statement PDF.
-- One-to-many with tenders.
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_bidders (
    id                  SERIAL PRIMARY KEY,
    tender_id           UUID NOT NULL
                            REFERENCES tenders(id) ON DELETE CASCADE,
    reference_number    TEXT,

    -- ── Bidder Identity ───────────────────────────────────
    bidder_name         TEXT NOT NULL,
    bidder_name_clean   TEXT,
    -- Normalised: "adani green energy" (lowercase, no suffixes)
    bidder_pan          TEXT,
    bidder_gst          TEXT,
    bidder_cin          TEXT,

    -- ── Consortium ────────────────────────────────────────
    is_consortium       BOOLEAN DEFAULT FALSE,
    consortium_lead     TEXT,
    consortium_members  JSONB,
    -- [{"name":"XYZ Pvt Ltd","role":"lead"},
    --  {"name":"ABC Ltd","role":"member"}]

    -- ── Bid Ranking ───────────────────────────────────────
    bid_rank            INTEGER,
    -- 1 = L1 (lowest/winning), 2 = L2, 3 = L3 ...
    quoted_tariff       NUMERIC(8,4),
    -- ₹/unit quoted by this bidder
    quoted_value        BIGINT,
    -- Total bid value in ₹ (for non-tariff tenders)
    bid_valid           BOOLEAN DEFAULT TRUE,
    disqualified_reason TEXT,
    -- If bid was rejected: "Non-responsive" / "EMD not submitted"
    is_winner           BOOLEAN DEFAULT FALSE,

    -- ── Source ────────────────────────────────────────────
    source_pdf_url      TEXT,
    -- URL of the Bid Opening Statement PDF this was extracted from
    extracted_at        TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 8: tender_awards
-- Awarded contractor information — from Letter of Award PDF.
-- One-to-one with tenders (only for awarded tenders).
-- ============================================================
CREATE TABLE IF NOT EXISTS tender_awards (
    id                      SERIAL PRIMARY KEY,
    tender_id               UUID UNIQUE NOT NULL
                                REFERENCES tenders(id) ON DELETE CASCADE,
    reference_number        TEXT,

    -- ── Winner ────────────────────────────────────────────
    awarded_to              TEXT,
    -- Company name as on LoA
    awarded_to_clean        TEXT,
    -- Normalised for entity matching
    awarded_to_pan          TEXT,
    awarded_to_gst          TEXT,
    awarded_to_cin          TEXT,
    awarded_to_address      TEXT,
    awarded_to_state        TEXT,

    -- ── Consortium ────────────────────────────────────────
    is_consortium_award     BOOLEAN DEFAULT FALSE,
    consortium_members      JSONB,
    -- Full list of consortium members from LoA

    -- ── Value ─────────────────────────────────────────────
    awarded_value           BIGINT,
    awarded_value_display   TEXT,
    awarded_tariff          NUMERIC(8,4),
    -- L1 tariff / final negotiated tariff
    no_of_bids_received     INTEGER,

    -- ── LoA Details ───────────────────────────────────────
    loa_date                DATE,
    loa_number              TEXT,
    agreement_date          DATE,
    financial_closure_date  DATE,

    -- ── Source ────────────────────────────────────────────
    loa_pdf_url             TEXT,
    extracted_at            TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 9: raw_records (scraper safety net)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_records (
    id              SERIAL PRIMARY KEY,
    portal          TEXT NOT NULL,
    raw_data        JSONB NOT NULL,
    html_snapshot   TEXT,
    batch_id        TEXT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE,
    error_message   TEXT
);


-- ============================================================
-- TABLE 10: scraper_runs (pipeline log)
-- ============================================================
CREATE TABLE IF NOT EXISTS scraper_runs (
    id              SERIAL PRIMARY KEY,
    portal          TEXT NOT NULL,
    batch_id        TEXT,
    trigger_type    TEXT DEFAULT 'scheduled',
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT DEFAULT 'running',
    records_found   INTEGER DEFAULT 0,
    records_new     INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    pdfs_downloaded INTEGER DEFAULT 0,
    pdfs_parsed     INTEGER DEFAULT 0,
    error_message   TEXT,
    stage_reached   TEXT
);


-- ============================================================
-- TABLE 11: niche_config
-- ============================================================
CREATE TABLE IF NOT EXISTS niche_config (
    id              SERIAL PRIMARY KEY,
    niche_name      TEXT UNIQUE NOT NULL,
    display_name    TEXT NOT NULL,
    search_keywords TEXT[] NOT NULL,
    category_rules  JSONB NOT NULL,
    metadata_schema JSONB,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- TABLE 12: companies (contractor entity registry)
-- ============================================================
CREATE TABLE IF NOT EXISTS companies (
    id                      SERIAL PRIMARY KEY,
    name_clean              TEXT UNIQUE NOT NULL,
    name_variants           TEXT[],
    pan                     TEXT,
    gst                     TEXT,
    cin                     TEXT,
    company_type            TEXT,
    is_developer            BOOLEAN DEFAULT FALSE,
    is_epc                  BOOLEAN DEFAULT FALSE,
    is_om                   BOOLEAN DEFAULT FALSE,
    is_consultant           BOOLEAN DEFAULT FALSE,
    total_bids              INTEGER DEFAULT 0,
    total_wins              INTEGER DEFAULT 0,
    win_rate_pct            NUMERIC(5,2),
    total_capacity_won_mw   NUMERIC(10,2),
    avg_winning_tariff      NUMERIC(8,4),
    address                 TEXT,
    state                   TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- INDEXES
-- ============================================================

-- tenders
CREATE INDEX IF NOT EXISTS idx_tenders_portal    ON tenders(portal);
CREATE INDEX IF NOT EXISTS idx_tenders_category  ON tenders(category);
CREATE INDEX IF NOT EXISTS idx_tenders_ref       ON tenders(reference_number);
CREATE INDEX IF NOT EXISTS idx_tenders_batch     ON tenders(batch_id);
CREATE INDEX IF NOT EXISTS idx_tenders_hash      ON tenders(hash);
CREATE INDEX IF NOT EXISTS idx_tenders_created   ON tenders(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tenders_search    ON tenders USING GIN(search_vector);
CREATE INDEX IF NOT EXISTS idx_tenders_trgm      ON tenders USING GIN(title_clean gin_trgm_ops);

-- tender_details
CREATE INDEX IF NOT EXISTS idx_details_status    ON tender_details(status);
CREATE INDEX IF NOT EXISTS idx_details_state     ON tender_details(state);
CREATE INDEX IF NOT EXISTS idx_details_deadline  ON tender_details(deadline);
CREATE INDEX IF NOT EXISTS idx_details_notif     ON tender_details(notification_number);

-- tender_financial
CREATE INDEX IF NOT EXISTS idx_fin_emd           ON tender_financial(emd_amount);
CREATE INDEX IF NOT EXISTS idx_fin_tariff        ON tender_financial(tariff_ceiling);

-- tender_documents
CREATE INDEX IF NOT EXISTS idx_docs_type         ON tender_documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_docs_notif        ON tender_documents(notification_number);
CREATE INDEX IF NOT EXISTS idx_docs_downloaded   ON tender_documents(downloaded) WHERE downloaded = FALSE;
CREATE INDEX IF NOT EXISTS idx_docs_parsed       ON tender_documents(parsed) WHERE parsed = FALSE;
CREATE INDEX IF NOT EXISTS idx_docs_amendment    ON tender_documents(is_amendment) WHERE is_amendment = TRUE;

-- tender_technical
CREATE INDEX IF NOT EXISTS idx_tech_capacity     ON tender_technical(capacity_mw);
CREATE INDEX IF NOT EXISTS idx_tech_power        ON tender_technical(power_type);
CREATE INDEX IF NOT EXISTS idx_tech_connect      ON tender_technical(connectivity);

-- tender_bidders
CREATE INDEX IF NOT EXISTS idx_bid_name          ON tender_bidders(bidder_name_clean);
CREATE INDEX IF NOT EXISTS idx_bid_winner        ON tender_bidders(is_winner) WHERE is_winner = TRUE;
CREATE INDEX IF NOT EXISTS idx_bid_rank          ON tender_bidders(bid_rank);

-- tender_awards
CREATE INDEX IF NOT EXISTS idx_award_company     ON tender_awards(awarded_to_clean);

-- raw_records
CREATE INDEX IF NOT EXISTS idx_raw_unprocessed   ON raw_records(processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_raw_batch         ON raw_records(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_portal        ON raw_records(portal);


-- ============================================================
-- TRIGGERS
-- ============================================================

-- Auto-update full text search vector on tenders
CREATE OR REPLACE FUNCTION update_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.organization, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'C') ||
        setweight(to_tsvector('english', COALESCE(NEW.category, '')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tsvector_update ON tenders;
CREATE TRIGGER tsvector_update
    BEFORE INSERT OR UPDATE ON tenders
    FOR EACH ROW EXECUTE FUNCTION update_search_vector();


-- Auto-update updated_at on all tables
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_updated_at ON tenders;
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON tenders
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

DROP TRIGGER IF EXISTS set_updated_at_details ON tender_details;
CREATE TRIGGER set_updated_at_details
    BEFORE UPDATE ON tender_details
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

DROP TRIGGER IF EXISTS set_updated_at_financial ON tender_financial;
CREATE TRIGGER set_updated_at_financial
    BEFORE UPDATE ON tender_financial
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();

DROP TRIGGER IF EXISTS set_updated_at_technical ON tender_technical;
CREATE TRIGGER set_updated_at_technical
    BEFORE UPDATE ON tender_technical
    FOR EACH ROW EXECUTE FUNCTION update_timestamp();


-- ============================================================
-- SEED DATA
-- ============================================================
INSERT INTO niche_config (niche_name, display_name, search_keywords, category_rules)
VALUES (
    'solar_bess',
    'Solar + BESS',
    ARRAY[
        'solar', 'solar PV', 'BESS', 'battery energy storage',
        'solar power plant', 'rooftop solar', 'solar hybrid',
        'energy storage system', 'solar module', 'solar EPC',
        'wind', 'hybrid RE', 'RTC power', 'round the clock',
        'green hydrogen', 'electrolyser', 'O&M', 'operation maintenance'
    ],
    '{
        "rules": [
            {"keywords": ["solar","bess"],          "match":"all", "category":"Solar+BESS Hybrid"},
            {"keywords": ["solar","battery"],        "match":"all", "category":"Solar+BESS Hybrid"},
            {"keywords": ["round the clock","rtc"],  "match":"any", "category":"Hybrid RE"},
            {"keywords": ["bess","energy storage"],  "match":"any", "category":"BESS Only"},
            {"keywords": ["solar pv","solar power","rooftop solar","photovoltaic"], "match":"any", "category":"Solar PV"},
            {"keywords": ["wind energy","wind power","wind farm"], "match":"any", "category":"Wind"},
            {"keywords": ["green hydrogen","electrolyser"], "match":"any", "category":"Green Hydrogen"},
            {"keywords": ["o&m","operation","maintenance","amc"], "match":"any", "category":"O&M"},
            {"keywords": ["consultancy","consulting","pmc"], "match":"any", "category":"Consultancy"},
            {"keywords": ["epc","procurement","construction"], "match":"any", "category":"EPC"},
            {"keywords": ["transmission","substation","cable"], "match":"any", "category":"Transmission"}
        ]
    }'::jsonb
) ON CONFLICT (niche_name) DO NOTHING;


-- ============================================================
-- VIEWS
-- ============================================================

-- Full tender view — single query for everything
CREATE OR REPLACE VIEW v_tenders_full AS
SELECT
    t.id,
    t.portal,
    t.reference_number,
    t.title,
    t.description,
    t.category,
    t.organization,
    t.organization_short,
    t.tender_type,
    t.source_url,

    d.status,
    d.date_published,
    d.deadline,
    d.bid_opening_date,
    d.financial_bid_opening,
    d.notification_number,
    d.state,
    d.district,
    d.region,
    d.corrigendum_count,
    d.pre_bid_date,

    f.emd_amount,
    f.emd_raw_text,
    f.tender_fee,
    f.sd_percentage,
    f.pg_percentage,
    f.tariff_ceiling,
    f.l1_tariff,
    f.estimated_value,

    tc.capacity_mw,
    tc.capacity_mwh,
    tc.power_type,
    tc.connectivity,
    tc.no_of_covers,
    tc.bid_system_type,
    tc.reverse_auction,
    tc.consortium_allowed,
    tc.net_worth_required_cr,
    tc.experience_required_mw,
    tc.ppa_duration_years,
    tc.domestic_content_req,

    a.awarded_to,
    a.awarded_value,
    a.awarded_tariff,
    a.no_of_bids_received,
    a.loa_date,

    t.created_at

FROM tenders t
LEFT JOIN tender_details    d  ON d.tender_id  = t.id
LEFT JOIN tender_financial  f  ON f.tender_id  = t.id
LEFT JOIN tender_technical  tc ON tc.tender_id = t.id
LEFT JOIN tender_awards     a  ON a.tender_id  = t.id;


-- Live open tenders only
CREATE OR REPLACE VIEW v_live_tenders AS
SELECT
    t.portal,
    t.reference_number,
    t.title,
    t.category,
    t.organization_short,
    d.state,
    d.deadline,
    d.notification_number,
    d.pre_bid_date,
    EXTRACT(DAYS FROM (d.deadline - NOW()))::INT AS days_left,
    f.emd_amount,
    f.tariff_ceiling,
    tc.capacity_mw,
    tc.power_type,
    tc.bid_system_type,
    tc.no_of_covers
FROM tenders t
JOIN tender_details    d  ON d.tender_id  = t.id AND d.status = 'open'
LEFT JOIN tender_financial  f  ON f.tender_id  = t.id
LEFT JOIN tender_technical  tc ON tc.tender_id = t.id
WHERE d.deadline > NOW()
ORDER BY d.deadline ASC;


-- Bidder leaderboard
CREATE OR REPLACE VIEW v_bidder_leaderboard AS
SELECT
    bidder_name_clean,
    COUNT(*)                                         AS total_bids,
    SUM(CASE WHEN is_winner THEN 1 ELSE 0 END)       AS total_wins,
    ROUND(
        SUM(CASE WHEN is_winner THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                AS win_rate_pct,
    ROUND(AVG(CASE WHEN is_winner THEN quoted_tariff END), 4)
                                                     AS avg_winning_tariff,
    MIN(quoted_tariff)                               AS lowest_tariff_ever
FROM tender_bidders
WHERE bidder_name_clean IS NOT NULL
GROUP BY bidder_name_clean
ORDER BY total_wins DESC;


-- ============================================================
-- VERIFY
-- ============================================================
\echo ''
\echo '=== Tables ==='
\dt

\echo ''
\echo '=== Views ==='
\dv

\echo ''
\echo '=== Schema v3 Corrected — Ready ==='