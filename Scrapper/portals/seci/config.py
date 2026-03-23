"""
portals/seci/config.py
----------------------
All SECI-specific settings live here.

When SECI changes their website, you update THIS file.
The scraper reads from here — it never hardcodes URLs or selectors.
"""

# Portal identity
PORTAL_NAME = "seci"
PORTAL_FULL_NAME = "Solar Energy Corporation of India"
PORTAL_SHORT = "SECI"

# URLs
BASE_URL = "https://www.seci.co.in"
TENDERS_URL = "https://www.seci.co.in/tenders"

# Rate limiting — seconds to wait between requests
# Government sites are slow. Be polite.
RATE_LIMIT_SECONDS = 2.5

# Page load timeout in milliseconds (30 seconds)
PAGE_TIMEOUT_MS = 30_000

# How long to wait for the tender table to appear (10 seconds)
TABLE_WAIT_MS = 10_000

# CSS selectors — what to look for on the page
# If SECI redesigns their site, update ONLY these selectors
SELECTORS = {
    # The main tender listing table
    "tender_table": "table tbody tr",

    # Header row (to build dynamic column map)
    "table_headers": "table thead th",

    # Detail page link inside a row
    "detail_link": "a[href*='tender-details'], a[href*='tenderdetails']",

    # Fallback: if table layout changes, try these
    "fallback_items": ".tender-item, .tender-row, .list-group-item",
}

# Browser settings
BROWSER_OPTIONS = {
    "headless": True,  # Set to False for debugging (opens visible browser)
    "slow_mo": 100,    # Milliseconds between actions (helps with flaky pages)
}
