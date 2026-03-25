"""
portals/seci/scraper.py
-----------------------
SECI portal scraper. Extends BasePortalScraper.

This file only contains SECI-specific parsing logic:
  - extract_listing_rows()  : reads SECI's DataTable using field_map
  - extract_detail_data()   : reads SECI's 2-column detail tables

Everything else (browser, pagination, rate limiting, DB saves,
retry logic, snapshot saving) is inherited from BasePortalScraper.
"""

from datetime import datetime

from playwright.async_api import Page

from core.base_scraper import BasePortalScraper
from portals.seci.config import (
    PORTAL_NAME, PORTAL_SHORT, BASE_URL, PAGES_TO_SCRAPE,
    RATE_LIMIT_SECONDS, PAGE_TIMEOUT_MS, BROWSER_OPTIONS, TEST_MAX_PAGES,
)
from portals.seci.field_map import build_column_index


class SECIScraper(BasePortalScraper):

    # ── Wire up config ────────────────────────────────────────
    PORTAL_NAME = PORTAL_NAME
    PORTAL_SHORT = PORTAL_SHORT
    BASE_URL = BASE_URL
    PAGES_TO_SCRAPE = PAGES_TO_SCRAPE
    RATE_LIMIT_SECONDS = RATE_LIMIT_SECONDS
    PAGE_TIMEOUT_MS = PAGE_TIMEOUT_MS
    BROWSER_HEADLESS = BROWSER_OPTIONS["headless"]
    MAX_PAGES = TEST_MAX_PAGES   # 0 = unlimited for production

    # ════════════════════════════════════════════════════════
    # SECI LISTING EXTRACTION
    # Uses field_map.build_column_index() — NOT hardcoded positions.
    # If SECI rearranges their columns, this still works.
    # ════════════════════════════════════════════════════════

    async def extract_listing_rows(
        self, page: Page, tender_status: str, source_url: str
    ) -> list[dict]:

        # Step 1: Build column map from the actual header row
        col_index = {}
        headers = await page.query_selector_all(
            "#tender-list thead th, table thead th"
        )
        if headers:
            header_texts = [(await h.inner_text()).strip() for h in headers]
            col_index = build_column_index(header_texts)
            if col_index:
                print(f"    Column map: {col_index}")
            else:
                print(f"    [WARNING] No columns mapped. Headers found: {header_texts}")

        # Step 2: Extract rows
        rows = await page.query_selector_all(
            "#tender-list tbody tr, table tbody tr"
        )
        extracted = []

        for i, row in enumerate(rows):
            try:
                cells = await row.query_selector_all("td")
                if not cells or len(cells) < 2:
                    continue

                cell_texts = [(await c.inner_text()).strip() for c in cells]
                full_text = " ".join(cell_texts)
                if len(full_text) < 10:
                    continue

                raw_record = {
                    "row_index": i,
                    "cell_texts": cell_texts,
                    "cell_count": len(cell_texts),
                    "tender_status": tender_status,
                    "source_url": source_url,
                    "scraped_at": datetime.utcnow().isoformat(),
                }

                # Map columns by header name — NOT by hardcoded position
                for field_name, col_idx in col_index.items():
                    if col_idx < len(cell_texts):
                        raw_record[field_name] = cell_texts[col_idx]

                # Fallback: if title wasn't mapped, use longest cell
                if "title" not in raw_record:
                    longest = max(cell_texts, key=len) if cell_texts else ""
                    if len(longest) > 15:
                        raw_record["title"] = longest[:500]

                # Clean multi-line reference numbers (SECI puts ref on 2 lines)
                if "reference_number" in raw_record:
                    ref = raw_record["reference_number"]
                    if "\n" in ref:
                        lines = [line.strip() for line in ref.split("\n") if line.strip()]
                        raw_record["reference_number"] = lines[-1] if lines else ref

                # Extract detail page link
                links = await row.query_selector_all("a[href]")
                for link in links:
                    href = await link.get_attribute("href") or ""
                    if "tender-details" in href or "tenderdetails" in href:
                        if href.startswith("/"):
                            href = f"{BASE_URL}{href}"
                        raw_record["detail_url"] = href
                        break

                extracted.append(raw_record)

            except Exception as e:
                print(f"    [ROW ERROR] Row {i}: {e}")
                continue

        return extracted

    # ════════════════════════════════════════════════════════
    # SECI DETAIL PAGE EXTRACTION
    # Uses base class shared utilities.
    # ════════════════════════════════════════════════════════

    async def extract_detail_data(self, page: Page, detail_url: str) -> dict:
        """
        SECI detail pages have:
          1. A 2-column label/value table (dates, EMD, CPPP ID, etc.)
          2. A "Tender Documents" section with downloadable files
          3. A "Tender Related Announcements" section (corrigenda)

        SECI sometimes swaps label and value — the normalizer handles this
        via get_detail_value() which checks both orientations.
        """
        # Step 1: Extract all key-value pairs from the 2-column tables
        detail = await self.extract_table_key_values(page)

        # Step 2: Extract tender documents section
        detail["documents"] = await self.extract_document_links(
            page, section_title="Tender Documents"
        )

        # Step 3: Extract corrigenda / announcements section
        detail["announcements"] = await self.extract_document_links(
            page, section_title="Tender Related Announcements"
        )

        doc_count = len(detail.get("documents", [])) + len(detail.get("announcements", []))
        if doc_count:
            print(f"      {doc_count} documents found")

        return detail


# ── Entry point for pipeline.py ───────────────────────────────────────

def scrape(conn, batch_id: str) -> int:
    """
    Called by pipeline.py as:
        scraper_mod.scrape(conn=conn, batch_id=batch_id)
    """
    return SECIScraper().scrape(conn, batch_id)
