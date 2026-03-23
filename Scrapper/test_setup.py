"""
test_setup.py
-------------
Run this AFTER completing all setup steps.
It checks every component of your system and tells you what's working and what's broken.

USAGE:
  python test_setup.py

You should see all green checkmarks (✓). If any fail (✗), follow the instructions.
"""

import sys
import os
import importlib

# Track results
tests_passed = 0
tests_failed = 0
total_tests = 0


def test(name, func):
    """Run a single test and report result."""
    global tests_passed, tests_failed, total_tests
    total_tests += 1
    try:
        func()
        tests_passed += 1
        print(f"  ✓ {name}")
        return True
    except Exception as e:
        tests_failed += 1
        print(f"  ✗ {name}")
        print(f"    → Error: {e}")
        return False


# ═══════════════════════════════════════════════════════════
# TEST 1: Python version
# ═══════════════════════════════════════════════════════════
print("\n═══ 1. Python Environment ═══")

def check_python_version():
    v = sys.version_info
    assert v.major == 3 and v.minor >= 10, f"Need Python 3.10+, got {v.major}.{v.minor}"

test("Python version >= 3.10", check_python_version)

def check_venv():
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    assert in_venv, (
        "Virtual environment NOT active! Run:\n"
        "    .\\venv\\Scripts\\Activate.ps1"
    )

test("Virtual environment is active", check_venv)


# ═══════════════════════════════════════════════════════════
# TEST 2: Required packages
# ═══════════════════════════════════════════════════════════
print("\n═══ 2. Required Packages ═══")

packages = [
    ("playwright", "playwright"),
    ("beautifulsoup4", "bs4"),
    ("psycopg2-binary", "psycopg2"),
    ("python-dateutil", "dateutil"),
    ("pdfplumber", "pdfplumber"),
    ("requests", "requests"),
    ("python-dotenv", "dotenv"),
]

for pkg_name, import_name in packages:
    def make_check(name):
        def check():
            importlib.import_module(name)
        return check
    test(f"Package: {pkg_name}", make_check(import_name))


# ═══════════════════════════════════════════════════════════
# TEST 3: Playwright browsers
# ═══════════════════════════════════════════════════════════
print("\n═══ 3. Playwright Browser ═══")

def check_playwright_chromium():
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://example.com", timeout=15000)
        title = page.title()
        browser.close()
        assert "Example" in title, f"Page title unexpected: {title}"

test("Playwright Chromium works (loads example.com)", check_playwright_chromium)


# ═══════════════════════════════════════════════════════════
# TEST 4: Project structure
# ═══════════════════════════════════════════════════════════
print("\n═══ 4. Project Structure ═══")

required_files = [
    ".env",
    ".gitignore",
    "requirements.txt",
    "main.py",
    "core/__init__.py",
    "core/db.py",
    "core/alerts.py",
    "core/retry.py",
    "core/pipeline.py",
    "core/date_parser.py",
    "core/value_parser.py",
    "portals/__init__.py",
    "portals/seci/__init__.py",
    "portals/seci/config.py",
    "portals/seci/field_map.py",
    "portals/seci/scraper.py",
    "portals/seci/normalizer.py",
]

for filepath in required_files:
    def make_check(fp):
        def check():
            assert os.path.exists(fp), f"File not found: {fp}"
        return check
    test(f"File exists: {filepath}", make_check(filepath))

required_dirs = [
    "storage/raw_snapshots",
    "storage/pdfs",
    "logs",
]

for dirpath in required_dirs:
    def make_check(dp):
        def check():
            assert os.path.isdir(dp), f"Directory not found: {dp}"
        return check
    test(f"Directory exists: {dirpath}", make_check(dirpath))


# ═══════════════════════════════════════════════════════════
# TEST 5: .env configuration
# ═══════════════════════════════════════════════════════════
print("\n═══ 5. Environment Configuration ═══")

def check_env_file():
    from dotenv import load_dotenv
    load_dotenv()
    password = os.getenv("DATABASE_PASSWORD")
    assert password, "DATABASE_PASSWORD not set in .env"
    assert password != "CHANGE_ME_TO_YOUR_REAL_PASSWORD", (
        "You haven't changed the default password in .env!\n"
        "    Open .env and set DATABASE_PASSWORD to your real PostgreSQL password."
    )

test(".env has real DATABASE_PASSWORD", check_env_file)


# ═══════════════════════════════════════════════════════════
# TEST 6: Database connection
# ═══════════════════════════════════════════════════════════
print("\n═══ 6. Database Connection ═══")

def check_db_connection():
    from core.db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    assert result == 1

test("PostgreSQL connection works", check_db_connection)


def check_tables_exist():
    from core.db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    required_tables = ["tenders", "raw_records", "scraper_runs"]
    missing = [t for t in required_tables if t not in tables]
    assert not missing, (
        f"Missing tables: {missing}\n"
        f"    Found: {tables}\n"
        f"    Run setup_db.sql to create tables."
    )

test("Required database tables exist", check_tables_exist)


# ═══════════════════════════════════════════════════════════
# TEST 7: Core modules
# ═══════════════════════════════════════════════════════════
print("\n═══ 7. Core Modules ═══")

def check_date_parser():
    from core.date_parser import parse_date
    result = parse_date("10/03/2026")
    from datetime import date
    assert result == date(2026, 3, 10), f"Expected 2026-03-10, got {result}"

test("Date parser (DD/MM/YYYY)", check_date_parser)

def check_value_parser():
    from core.value_parser import parse_amount
    result = parse_amount("INR 1,42,50,000")
    assert result == 14250000, f"Expected 14250000, got {result}"

test("Value parser (INR notation)", check_value_parser)

def check_retry_module():
    from core.retry import retry_async, retry_sync
    assert callable(retry_async)
    assert callable(retry_sync)

test("Retry module loads", check_retry_module)

def check_alerts_module():
    from core.alerts import send_alert, alert_success, alert_error
    assert callable(send_alert)

test("Alerts module loads", check_alerts_module)


# ═══════════════════════════════════════════════════════════
# TEST 8: Portal modules
# ═══════════════════════════════════════════════════════════
print("\n═══ 8. SECI Portal ═══")

def check_seci_config():
    from portals.seci.config import PORTAL_NAME, TENDERS_URL
    assert PORTAL_NAME == "seci"
    assert "seci.co.in" in TENDERS_URL

test("SECI config loads", check_seci_config)

def check_seci_field_map():
    from portals.seci.field_map import build_column_index
    headers = ["S.No", "Tender ID", "Tender Ref No.", "Tender Title", "Publication Date"]
    result = build_column_index(headers)
    assert "title" in result, f"Title not found in column map: {result}"

test("SECI field map works", check_seci_field_map)

def check_seci_scraper():
    from portals.seci.scraper import scrape
    assert callable(scrape)

test("SECI scraper module loads", check_seci_scraper)

def check_seci_normalizer():
    from portals.seci.normalizer import normalize, classify_tender
    assert classify_tender("Solar PV Project") == "Solar PV"
    assert classify_tender("BESS Storage") == "BESS Only"

test("SECI normalizer + classifier works", check_seci_normalizer)


# ═══════════════════════════════════════════════════════════
# RESULTS
# ═══════════════════════════════════════════════════════════
print(f"\n{'='*50}")
print(f"  RESULTS: {tests_passed}/{total_tests} tests passed")
if tests_failed == 0:
    print(f"  ✓ ALL TESTS PASSED! Your setup is complete.")
    print(f"  ")
    print(f"  NEXT STEP: Run the SECI scraper:")
    print(f"    python main.py --portal seci")
else:
    print(f"  ✗ {tests_failed} tests FAILED. Fix the issues above and run again.")
print(f"{'='*50}\n")
