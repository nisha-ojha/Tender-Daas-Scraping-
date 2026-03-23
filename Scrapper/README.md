# Tender DAAS — Scraping Pipeline

Automated tender scraping system for Indian government portals (SECI, CPPP, State portals).

## Quick Setup (Windows)

### Step 1: Create virtual environment
```powershell
cd "A:\Tender DAAS\Scrapper"
python -m venv venv
.\venv\Scripts\Activate.ps1
```

### Step 2: Install dependencies
```powershell
pip install -r requirements.txt
playwright install chromium
```

### Step 3: Set up database
Edit `.env` with your PostgreSQL password, then:
```powershell
psql -U postgres -f setup_db.sql
```
Or run Step A and Step B manually (see inside setup_db.sql).

### Step 4: Verify everything
```powershell
python test_setup.py
```

### Step 5: Run the scraper
```powershell
python main.py --portal seci
```

## Project Structure
```
Scrapper/
├── main.py                 ← Run this
├── core/                   ← Shared code (db, alerts, retry)
├── portals/seci/           ← SECI-specific scraper + normalizer
├── portals/cppp/           ← CPPP (build in Week 3)
├── storage/                ← HTML snapshots + PDFs
├── logs/                   ← Runtime logs
└── dashboard/              ← Monitoring API (build in Week 4)
```

## Commands
| What | Command |
|------|---------|
| Run SECI scraper | `python main.py --portal seci` |
| Run only scrape stage | `python main.py --portal seci --stage scrape` |
| Run all portals | `python main.py --portal all` |
| Test setup | `python test_setup.py` |
| Check DB | `psql -U tender_user -d tender_db -h localhost` |
