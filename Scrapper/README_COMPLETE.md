# Tender DAAS — Automated Tender Scraping Platform

Automated scraper for Indian government tender portals (SECI, CPPP, State portals). Extracts tender data, normalizes it into PostgreSQL, deduplicates entries, and provides a real-time monitoring dashboard.

**Status**: ✅ SECI portal fully working | Dashboard operational | Infrastructure complete

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- PostgreSQL 12+
- Windows (tested on Windows 10/11)

### Setup (5 minutes)

```powershell
# 1. Navigate to project
cd "A:\Tender DAAS\Scrapper"

# 2. Create virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 4. Configure database (edit .env with your PostgreSQL password)
psql -U postgres -f setup_db.sql

# 5. Verify setup
python test_setup.py

# 6. Run the scraper
python main.py --portal seci

# 7. View dashboard (in another terminal)
uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload
# Open: http://localhost:8000
```

---

## 📋 What It Does

### 3-Stage Pipeline

**Stage 1: SCRAPE** (`portals/seci/scraper.py`)
- Two-phase approach
- **Phase 1 (Fast)**: Pagination loop collects listing data (title, ref, dates)
- **Phase 2 (Slow)**: Visit each tender's detail page for EMD, dates, documents
- Saves raw data to `raw_records` table
- Rate-limited to 2.5s between requests

**Stage 2: NORMALIZE** (`portals/seci/normalizer.py`)
- Transform raw data into clean, structured format
- Parse dates (handle "TBD", various formats)
- Parse amounts (rupees, crores, lakhs)
- Check for duplicates by reference_number + organization
- Insert into `tenders` table

**Stage 3: DEDUPLICATE** (TODO)
- Identify cross-portal duplicate tenders
- Flag to `review_queue` for manual review

### Database

PostgreSQL with 6 tables:
- **tenders** — Normalized tender data (main)
- **raw_records** — Raw scraped data (safety net)
- **scraper_runs** — Pipeline execution log
- **tender_changes** — Change history
- **review_queue** — Duplicates for manual review
- **niche_config** — Categorization rules

Full schema in `setup_db.sql`

### Dashboard

Real-time monitoring at `http://localhost:8000`

**Features**:
- Stats cards (total, open, closed, awarded)
- Tenders by status & category
- Data quality metrics
- Recent scraper runs
- Error logs
- Recently added tenders
- Auto-refresh every 30 seconds

**API Endpoints**:
- `GET /api/stats` — Overall statistics
- `GET /api/runs` — Recent pipeline runs
- `GET /api/recent-tenders` — Last 30 tenders
- `GET /api/errors` — Recent errors
- `GET /api/quality` — Data quality metrics

---

## 📁 Project Structure

```
Scrapper/
├── main.py                    ← ENTRY POINT
├── requirements.txt           ← Dependencies
├── setup_db.sql              ← Database schema
├── test_setup.py             ← Verify installation
├── .env                       ← Configuration (secret)
├── .env.example              ← Config template
│
├── core/                      ← Shared utilities (all portals use)
│   ├── db.py                 ← Database operations (single source of truth)
│   ├── pipeline.py           ← 3-stage orchestration
│   ├── alerts.py             ← Webhook notifications
│   ├── retry.py              ← Exponential backoff
│   ├── date_parser.py        ← Date parsing
│   └── value_parser.py       ← Currency parsing
│
├── portals/                   ← Portal-specific scrapers (plug & play)
│   ├── seci/                 ← Solar Energy Corporation India
│   │   ├── scraper.py        ← Two-phase scraper
│   │   ├── normalizer.py     ← Data transformation
│   │   ├── config.py         ← URLs, selectors, settings
│   │   └── field_map.py      ← Column mapping
│   │
│   └── cppp/                 ← Central Portal for Public Procurement (TODO)
│
├── dashboard/                 ← Monitoring UI & API
│   └── app.py                ← FastAPI application
│
├── deduplicator/             ← Duplicate detection (Stage 3 - TODO)
├── storage/                  ← Local cache (HTML, PDFs)
├── logs/                     ← Runtime logs
└── venv/                     ← Python virtual environment
```

---

## 🎮 Commands

```powershell
# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Run SECI scraper (all stages)
python main.py --portal seci

# Run only scrape stage
python main.py --portal seci --stage scrape

# Run only normalize stage
python main.py --portal seci --stage normalize

# Run all portals (when CPPP is ready)
python main.py --portal all

# Test setup
python test_setup.py

# Start dashboard
uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload

# Check database
psql -U tender_user -d tender_db -h localhost
```

---

## ⚙️ Configuration

Edit `.env` file:

```
# Database
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=tender_db
DATABASE_USER=tender_user
DATABASE_PASSWORD=your_password

# Webhook Alerts (Discord/Slack/Telegram)
WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN

# Proxy (optional)
PROXY_URL=
```

---

## 🔧 Architecture

### Key Design Principles

1. **Atomic Transactions**
   - All 3 stages run in ONE database transaction
   - If ANY stage fails → ROLLBACK everything
   - No partial/corrupted data, ever

2. **Single Source of Truth**
   - ALL database operations go through `core/db.py`
   - Portal-specific code NEVER talks to DB directly
   - Makes it easy to swap database backend

3. **Portal Pluggability**
   - Each portal is a folder: `portals/{name}/`
   - Must have: `scraper.py`, `normalizer.py`, `config.py`
   - System auto-discovers portals via importlib
   - Add new portal without touching core code

4. **Never Crash the System**
   - If one portal fails → log it, continue to next
   - If one record fails → log error, continue batch
   - If webhook is down → log warning, don't crash scraper

---

## 🗄️ Database Schema (Quick Reference)

### `tenders` table

```sql
id (UUID)                    -- Primary key
reference_number (TEXT)      -- "SECI/S/2026/001"
title (TEXT)                 -- Tender title
organization (TEXT)          -- "Solar Energy Corporation of India"
organization_short (TEXT)    -- "SECI"
value (BIGINT)               -- Amount in rupees
value_display (TEXT)         -- "₹10 Cr"
emd_amount (BIGINT)          -- Earnest Money Deposit
date_published (DATE)        -- Publication date
deadline (TIMESTAMPTZ)       -- Bid submission deadline
category (TEXT)              -- "Solar PV", "BESS", "Uncategorized"
status (TEXT)                -- "open", "closed", "awarded"
document_urls (TEXT[])       -- Array of PDFs
source_portal (TEXT)         -- "seci", "cppp"
niche_metadata (JSONB)       -- Flexible sector-specific data
batch_id (TEXT)              -- Pipeline run ID
created_at, updated_at       -- Timestamps
```

### Other tables
- **raw_records** — Unprocessed raw data (fallback)
- **scraper_runs** — Audit log of pipeline runs
- **tender_changes** — Track field changes
- **review_queue** — Duplicate candidates
- **niche_config** — Categorization rules

Full schema: `setup_db.sql`

---

## 📊 Dashboard API Examples

```bash
# Get overall stats
curl http://localhost:8000/api/stats

# Get recent runs
curl http://localhost:8000/api/runs

# Get recent errors
curl http://localhost:8000/api/errors

# Get data quality metrics
curl http://localhost:8000/api/quality

# Get recently added tenders
curl http://localhost:8000/api/recent-tenders
```

---

## 🔍 Debugging

### Check database connection
```powershell
python core/db.py
```

### Check alerts
```powershell
python core/alerts.py
```

### View database directly
```bash
psql -U tender_user -d tender_db

# View recent tenders
SELECT reference_number, title, status FROM tenders LIMIT 10;

# View raw records with errors
SELECT * FROM raw_records WHERE error_message IS NOT NULL;

# View pipeline runs
SELECT portal, batch_id, status, records_found FROM scraper_runs 
ORDER BY started_at DESC LIMIT 5;
```

### See browser during scraping
Edit `portals/seci/config.py`:
```python
BROWSER_OPTIONS = {
    "headless": False,  # ← Change to False
    "slow_mo": 500,
}
```

---

## 🆕 Adding a New Portal

### Step 1: Create folder structure
```
portals/new_portal/
├── scraper.py
├── normalizer.py
├── config.py
└── __init__.py
```

### Step 2: Implement `scraper.py`
```python
def scrape(conn, batch_id):
    """Entry point from pipeline.py"""
    from core.db import insert_raw_record
    
    # Your scraping logic here
    total_records = 0
    for tender in scraped_tenders:
        insert_raw_record(conn, "new_portal", tender, batch_id)
        total_records += 1
    
    return total_records
```

### Step 3: Implement `normalizer.py`
```python
from core.db import (
    get_unprocessed_raw_records,
    insert_tender,
    mark_raw_record_processed,
)

def normalize(conn, batch_id):
    """Entry point from pipeline.py"""
    result = {"new": 0, "updated": 0, "errors": 0}
    
    raw_records = get_unprocessed_raw_records(conn, "new_portal", batch_id)
    
    for record in raw_records:
        try:
            tender_data = transform_raw_to_tender(record["raw_data"])
            tender_id = insert_tender(conn, tender_data)
            if tender_id:
                result["new"] += 1
            mark_raw_record_processed(conn, record["id"])
        except Exception as e:
            result["errors"] += 1
            mark_raw_record_processed(conn, record["id"], str(e))
    
    return result
```

### Step 4: Update `main.py`
```python
AVAILABLE_PORTALS = ["seci", "new_portal"]
```

### Step 5: Run
```powershell
python main.py --portal new_portal
```

---

## 📦 Dependencies

```
playwright==1.52.0          # Browser automation
beautifulsoup4==4.13.4      # HTML parsing
psycopg2-binary==2.9.10     # PostgreSQL driver
python-dateutil==2.9.0      # Date parsing
pdfplumber==0.11.6          # PDF extraction
requests==2.32.3            # HTTP + webhooks
python-dotenv==1.1.0        # .env loading
schedule==1.2.2             # Scheduling (optional)
fastapi==0.115.0            # Dashboard API
uvicorn==0.30.0             # ASGI server
```

---

## 🎯 Core Modules

### `core/db.py` — Database Operations
All DB operations go through here. Never import DB driver directly.

**Key functions**:
- `get_connection()` — Get DB connection
- `insert_raw_record(conn, portal, raw_data, batch_id)` — Save scraped data
- `insert_tender(conn, tender_data)` — Insert normalized tender
- `get_unprocessed_raw_records(conn, portal, batch_id)` — Fetch raw records
- `mark_raw_record_processed(conn, record_id, error_message)` — Mark done
- `log_scraper_run(conn, portal, batch_id, status, ...)` — Audit log

### `core/pipeline.py` — Pipeline Orchestration
Runs the 3-stage pipeline with transaction handling.

**Key functions**:
- `run_pipeline(portal, batch_id, stages="all")` — Main entry point

### `core/alerts.py` — Webhook Notifications
Send alerts to Discord/Slack/Telegram.

**Key functions**:
- `alert_success(portal, message, batch_id)`
- `alert_error(portal, message, batch_id)`
- `alert_info(portal, message, batch_id)`

### `core/date_parser.py` — Date Parsing
Handles Indian tender date formats: "10-Jan-2026", "10/01/2026", "TBD", etc.

### `core/value_parser.py` — Currency Parsing
Handles Indian amounts: "₹10 Cr", "10 L", "TBD", etc.

---

## 🚨 Error Handling

**Scraper fails?**
- Check logs in `logs/` folder
- Check database: `SELECT * FROM raw_records WHERE error_message IS NOT NULL`
- Enable `headless=False` in config.py to see browser
- Check `.env` configuration

**Database connection fails?**
- Is PostgreSQL running? (Check Windows Services)
- Is password in `.env` correct?
- Does database `tender_db` exist?
- Run `python core/db.py` to test

**Tender not inserted?**
- Check `raw_records` table for error message
- Check `scraper_runs` table for batch status
- Look at dashboard → Recent Errors

**Webhook not working?**
- Check WEBHOOK_URL in `.env`
- Don't worry — scraper continues even if webhook fails
- Alerts print to console anyway

---

## 📈 Next Features to Build

### Phase 2 (Weeks 2-3)
- [ ] CPPP scraper (Central Portal for Public Procurement)
- [ ] State portal scrapers (TN, UP, etc.)
- [ ] Document download and PDF extraction
- [ ] BOQ parsing from PDFs

### Phase 3 (Weeks 4-5)
- [ ] Deduplicator (Stage 3) — cross-portal duplicates
- [ ] Auto-categorizer (ML/keyword-based)
- [ ] Search API endpoint
- [ ] Notification preferences

### Phase 4 (Weeks 6+)
- [ ] Advanced filtering API
- [ ] Tender comparison
- [ ] Historical tracking
- [ ] Eligibility matching
- [ ] Integration with bid systems

---

## 💡 Tips & Best Practices

1. **Rate Limiting**: Change `RATE_LIMIT_SECONDS` in `portals/seci/config.py` (default 2.5s)

2. **Pagination Limit**: Change `TEST_MAX_PAGES` for testing (default 3, increase for production)

3. **Database Cleanup**: Delete old raw_records periodically
   ```sql
   DELETE FROM raw_records WHERE scraped_at < NOW() - INTERVAL '30 days';
   ```

4. **Full-Text Search**: Search the tenders table
   ```sql
   SELECT * FROM tenders 
   WHERE search_vector @@ plainto_tsquery('english', 'solar panel')
   LIMIT 10;
   ```

5. **Monitoring**: Check `scraper_runs` table for pipeline health
   ```sql
   SELECT portal, status, COUNT(*) FROM scraper_runs 
   GROUP BY portal, status;
   ```

---

## 📞 Support

- **Setup issues**: Check `test_setup.py` output
- **Database issues**: Test with `python core/db.py`
- **Scraper issues**: Check logs in `logs/` folder
- **Dashboard issues**: Check browser console
- **Code questions**: See comments in source files

---

## 📄 License

Built for Tender DAAS project. Handles Indian government tender portals.

---

## 🔗 Resources

- PostgreSQL docs: https://www.postgresql.org/docs/
- Playwright docs: https://playwright.dev/python/
- FastAPI docs: https://fastapi.tiangolo.com/
- BeautifulSoup docs: https://www.crummy.com/software/BeautifulSoup/
