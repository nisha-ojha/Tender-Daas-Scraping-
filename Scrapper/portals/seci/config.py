"""
portals/seci/config.py
----------------------
All SECI-specific settings live here.
"""

# Portal identity
PORTAL_NAME = "seci"
PORTAL_FULL_NAME = "Solar Energy Corporation of India"
PORTAL_SHORT = "SECI"

# URLs
BASE_URL = "https://www.seci.co.in"

# ── THREE SEPARATE PAGES (not tabs!) ──
# Each page has its own URL and tender status
PAGES_TO_SCRAPE = [
    {
        "name": "Live Tenders",
        "url": "https://www.seci.co.in/tenders",
        "tender_status": "live",     # → status='open' in database
    },
    {
        "name": "Archived Tenders",
        "url": "https://www.seci.co.in/tenders/archive",
        "tender_status": "archive",  # → status='closed' in database
    },
    {
        "name": "Tender Results",
        "url": "https://seci.co.in/tenders/results",
        "tender_status": "result",   # → status='awarded' in database
    },
]

# Rate limiting
RATE_LIMIT_SECONDS = 2.5

# Timeouts (milliseconds)
PAGE_TIMEOUT_MS = 60_000
TABLE_WAIT_MS = 10_000

# CSS Selectors for listing pages
SELECTORS = {
    "tender_table": "table tbody tr",
    "table_headers": "table thead th",
    "detail_link": "a[href*='tender-details'], a[href*='tenderdetails']",
}

# Labels to search for on the DETAIL page
DETAIL_SELECTORS = {
    "publication_date": "Tender Publication Date",
    "pre_bid_date": "Pre Bid Meeting Date",
    "bid_submission_online": "Bid Submission End Date (Online)",
    "bid_submission_offline": "Bid Submission End Date (Offline)",
    "bid_open_date": "Bid Open Date",
    "tender_description": "Tender Description",
    "emd_amount": "EMD",
    "tender_fee": "Tender Fee",
    "cppp_tender_id": "Tender ID On CPPP",
    "tender_type": "Tender Type",
}

# Browser settings
BROWSER_OPTIONS = {
    "headless": True,   # Set False to see browser for debugging
    "slow_mo": 100,
}

# Safety limit: max pages of pagination per URL
MAX_PAGES_PER_URL = 50
