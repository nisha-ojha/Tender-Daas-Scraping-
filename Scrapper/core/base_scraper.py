"""
core/base_scraper.py
--------------------
Abstract base class for ALL portal scrapers (SECI, CPPP, GeM, etc.)

HOW TO ADD A NEW PORTAL:
  1. Create portals/new_portal/scraper.py
  2. Import BasePortalScraper
  3. Set the class attributes (PORTAL_NAME, BASE_URL, etc.)
  4. Implement extract_listing_rows() and extract_detail_data()
  5. Add scrape() entry function at the bottom
  6. Done. pipeline.py auto-discovers it.

The base class handles:
  - Browser setup and teardown
  - Pagination loop (with duplicate-page detection)
  - DB existence check before visiting detail pages
  - Rate limiting between requests
  - Retry logic on page loads
  - Storage snapshot saving and cleanup
  - All shared Playwright utilities

Each portal subclass only handles:
  - Its own listing table parsing  (extract_listing_rows)
  - Its own detail page parsing    (extract_detail_data)
"""

import asyncio
import os
import random
import re
import shutil
from abc import ABC, abstractmethod
from datetime import datetime

from playwright.async_api import async_playwright, Page

from core.db import insert_raw_record, find_by_reference
from core.retry import retry_async


class BasePortalScraper(ABC):
    """
    Abstract base class. Subclasses set class attributes and implement
    the two abstract methods. Everything else is shared.
    """

    # ── Required: set these in every subclass ────────────────
    PORTAL_NAME: str = ""        # e.g. "seci"   — matches portals/ folder
    PORTAL_SHORT: str = ""       # e.g. "SECI"   — used for DB dedup check
    BASE_URL: str = ""           # e.g. "https://www.seci.co.in"
    PAGES_TO_SCRAPE: list = []   # list of {name, url, tender_status}

    # ── Optional: override in subclass if needed ─────────────
    RATE_LIMIT_SECONDS: float = 2.5
    PAGE_TIMEOUT_MS: int = 60_000
    BROWSER_HEADLESS: bool = True
    MAX_PAGES: int = 0           # 0 = unlimited (production). Set >0 for testing.
    USER_AGENT: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
    STORAGE_DIR: str = "storage"

    # ════════════════════════════════════════════════════════
    # PUBLIC ENTRY POINT — called by pipeline.py
    # ════════════════════════════════════════════════════════

    def scrape(self, conn, batch_id: str) -> int:
        """
        Main entry point. Called by pipeline.py as:
            scraper_mod.scrape(conn=conn, batch_id=batch_id)

        Cleans old snapshots, then runs the async pipeline.
        Returns the total number of raw_records saved.
        """
        self._clean_listing_snapshots()
        return asyncio.run(self._scrape_async(conn, batch_id))

    # ════════════════════════════════════════════════════════
    # ABSTRACT METHODS — each portal MUST implement both
    # ════════════════════════════════════════════════════════

    @abstractmethod
    async def extract_listing_rows(
        self, page: Page, tender_status: str, source_url: str
    ) -> list[dict]:
        """
        Extract all tender rows from the CURRENTLY VISIBLE listing page.
        Do NOT navigate away from this page — pagination is handled by base class.

        Each returned dict should include at minimum:
          title            (str)  — tender title
          reference_number (str)  — unique ref ID if visible on listing
          detail_url       (str)  — link to the tender's detail page
          tender_status    (str)  — passed through from page_config
          source_url       (str)  — the listing page URL
          scraped_at       (str)  — ISO timestamp
        """
        ...

    @abstractmethod
    async def extract_detail_data(self, page: Page, detail_url: str) -> dict:
        """
        The page has ALREADY been navigated to detail_url by the base class.
        Parse whatever fields are available and return as a flat dict.

        Use self.extract_table_key_values(page) as a starting point —
        it handles the common 2-column label/value table pattern.
        Add portal-specific parsing on top.
        """
        ...

    # ════════════════════════════════════════════════════════
    # OPTIONAL OVERRIDES — only if your portal needs them
    # ════════════════════════════════════════════════════════

    async def click_next_page(self, page: Page) -> bool:
        """
        Click the pagination Next button.
        Returns True if page changed, False if on last page.

        Default works for DataTable-style pagination (SECI, many portals).
        Override if your portal uses a different pagination pattern.
        """
        try:
            first_row = await page.query_selector("tbody tr td")
            old_text = (await first_row.inner_text()).strip() if first_row else ""

            btn = await page.query_selector(
                "button.dt-paging-button.next:not(.disabled), "
                "a.paginate_button.next:not(.disabled), "
                "li.next:not(.disabled) a"
            )

            if not btn:
                return False

            classes = await btn.get_attribute("class") or ""
            if "disabled" in classes:
                return False

            await btn.click()
            await asyncio.sleep(2)

            first_row_new = await page.query_selector("tbody tr td")
            new_text = (await first_row_new.inner_text()).strip() if first_row_new else ""

            if new_text == old_text:
                return False

            return True

        except Exception as e:
            print(f"  [PAGINATION ERROR] {e}")
            return False

    def should_visit_detail(self, raw_record: dict) -> bool:
        """
        Return True if this record needs a detail page visit.
        Default: visit if detail_url is present.
        Override for custom logic (e.g. skip result-page tenders).
        """
        return bool(raw_record.get("detail_url"))

    async def build_browser_context(self, browser):
        """
        Create a Playwright browser context.
        Override to add cookies, proxies, custom headers, etc.
        """
        return await browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent=self.USER_AGENT,
        )

    # ════════════════════════════════════════════════════════
    # INTERNAL ENGINE — don't override
    # ════════════════════════════════════════════════════════

    async def _scrape_async(self, conn, batch_id: str) -> int:
        total_saved = 0
        browser = None

        async with async_playwright() as pw:
            try:
                browser = await pw.chromium.launch(headless=self.BROWSER_HEADLESS)
                context = await self.build_browser_context(browser)
                page = await context.new_page()

                for page_config in self.PAGES_TO_SCRAPE:
                    page_name = page_config["name"]
                    page_url = page_config["url"]
                    tender_status = page_config["tender_status"]

                    print(f"\n{'━'*60}")
                    print(f"  {page_name.upper()}  ({page_url})")
                    print(f"{'━'*60}")

                    try:
                        count = await self._scrape_one_page(
                            page, conn, batch_id, page_url, tender_status
                        )
                        total_saved += count
                        print(f"\n  ✓ {page_name}: {count} records saved")
                    except Exception as e:
                        print(f"\n  ✗ {page_name} FAILED: {e}")
                        continue  # Try next page, don't abort everything

            finally:
                if browser:
                    try:
                        await browser.close()
                    except Exception:
                        pass

        print(f"\n{'='*60}")
        print(f"  {self.PORTAL_SHORT} TOTAL: {total_saved} records")
        print(f"{'='*60}")
        return total_saved

    async def _scrape_one_page(
        self,
        page: Page,
        conn,
        batch_id: str,
        page_url: str,
        tender_status: str,
    ) -> int:

        # ════════════════════════
        # PHASE 1: Pagination
        # ════════════════════════
        print(f"\n  ── PHASE 1: Collecting listing rows ──")

        async def load():
            await page.goto(
                page_url,
                timeout=self.PAGE_TIMEOUT_MS,
                wait_until="domcontentloaded",
            )
            await page.wait_for_selector("table", timeout=15_000)

        await retry_async(
            load, max_retries=3, base_delay=5, operation_name="Page load"
        )
        await asyncio.sleep(3)

        await self._save_snapshot(page, f"{self.PORTAL_SHORT}_{tender_status}_listing")

        all_rows: list[dict] = []
        page_num = 1
        seen_first_rows: set[str] = set()

        while True:
            # Respect MAX_PAGES limit (0 = unlimited)
            if self.MAX_PAGES and page_num > self.MAX_PAGES:
                print(f"  MAX_PAGES ({self.MAX_PAGES}) reached. Stopping.")
                break

            # Duplicate page detection — catches infinite loops
            first_cell = await page.query_selector("tbody tr td")
            first_text = (await first_cell.inner_text()).strip() if first_cell else ""

            if first_text and first_text in seen_first_rows:
                print(f"  Duplicate page detected. Stopping.")
                break
            seen_first_rows.add(first_text)

            print(f"  Page {page_num}:", end=" ", flush=True)
            rows = await self.extract_listing_rows(page, tender_status, page_url)
            print(f"{len(rows)} rows")
            all_rows.extend(rows)

            has_next = await self.click_next_page(page)
            if not has_next:
                print(f"  Last page reached.")
                break

            page_num += 1
            await asyncio.sleep(random.uniform(1.5, 2.5))

        print(f"\n  Phase 1 complete: {len(all_rows)} rows from {page_num} page(s)")

        if not all_rows:
            return 0

        # ════════════════════════
        # PHASE 2: Detail pages
        # ════════════════════════
        print(f"\n  ── PHASE 2: Visiting detail pages ──")

        saved = 0
        skipped = 0

        for idx, raw_record in enumerate(all_rows):
            ref = raw_record.get("reference_number", "")
            title_short = raw_record.get("title", "")[:50]
            progress = f"#{idx+1}/{len(all_rows)}"

            # Skip if already in DB — but still save listing data to raw_records
            if ref:
                existing = find_by_reference(conn, ref, self.PORTAL_SHORT)
                if existing:
                    insert_raw_record(conn, self.PORTAL_NAME, raw_record, batch_id)
                    skipped += 1
                    print(f"  {progress} [SKIP] {ref}")
                    continue

            # Visit detail page for new tenders
            if self.should_visit_detail(raw_record):
                detail_url = raw_record["detail_url"]
                print(f"  {progress} [NEW]  {title_short}...")
                detail = await self._visit_detail_page(page, detail_url)
                if detail:
                    raw_record["detail"] = detail

                await asyncio.sleep(
                    random.uniform(
                        self.RATE_LIMIT_SECONDS,
                        self.RATE_LIMIT_SECONDS + 1.5,
                    )
                )
            else:
                print(f"  {progress} [LIST] {title_short}")

            record_id = insert_raw_record(
                conn, self.PORTAL_NAME, raw_record, batch_id
            )
            if record_id:
                saved += 1

        print(
            f"\n  Phase 2 complete: {saved} new saved, {skipped} skipped (already in DB)"
        )
        return saved + skipped

    async def _visit_detail_page(self, page: Page, detail_url: str) -> dict:
        """
        Navigate to detail_url, call extract_detail_data(), return result.
        All error handling is here — the subclass method never needs try/except.
        """
        try:
            async def goto():
                await page.goto(
                    detail_url,
                    timeout=self.PAGE_TIMEOUT_MS,
                    wait_until="domcontentloaded",
                )

            await retry_async(
                goto, max_retries=2, base_delay=3, operation_name="Detail page"
            )
            await asyncio.sleep(1.5)

            return await self.extract_detail_data(page, detail_url)

        except Exception as e:
            print(f"    [DETAIL ERROR] {e}")
            return {"_error": str(e)[:200]}

    # ════════════════════════════════════════════════════════
    # SHARED UTILITIES — available to all subclasses via self.
    # ════════════════════════════════════════════════════════

    async def extract_table_key_values(self, page: Page) -> dict:
        """
        Extract all 2-column label→value table rows from the page.
        This is the most common pattern on government portal detail pages.

        Returns a flat dict: {"Tender ID": "SECI/2026/xxx", ...}
        """
        detail = {}
        rows = await page.query_selector_all("table tr")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) == 2:
                key = (await cells[0].inner_text()).strip().rstrip(":")
                value = (await cells[1].inner_text()).strip()
                if key and value and len(key) > 2 and len(key) < 120:
                    detail[key] = value

        return detail

    async def extract_document_links(
        self, page: Page, section_title: str = None
    ) -> list[dict]:
        """
        Extract document download links from the page.

        section_title: If given, only extract from that section's table
                       (e.g. "Tender Documents", "Tender Related Announcements")
                       If None, finds all PDF/download links on the page.
        """
        documents = []

        try:
            if section_title:
                headers = await page.query_selector_all("th, td, h3, h4, h5")
                target_table = None

                for header in headers:
                    text = await header.inner_text()
                    if section_title.lower() in text.lower():
                        target_table = await header.evaluate_handle("""
                            el => {
                                let t = el.closest('table');
                                if (t) return t;
                                let next = el.nextElementSibling;
                                while (next) {
                                    if (next.tagName === 'TABLE') return next;
                                    let inner = next.querySelector('table');
                                    if (inner) return inner;
                                    next = next.nextElementSibling;
                                }
                                return null;
                            }
                        """)
                        break

                if target_table:
                    rows = await target_table.query_selector_all("tr")
                    for row in rows:
                        links = await row.query_selector_all("a[href]")
                        cells = await row.query_selector_all("td")
                        for link in links:
                            href = await link.get_attribute("href") or ""
                            name = (await link.inner_text()).strip()
                            if href and len(name) > 2:
                                if href.startswith("/"):
                                    href = f"{self.BASE_URL}{href}"
                                doc = {"name": name, "url": href}
                                for cell in cells:
                                    ct = (await cell.inner_text()).strip()
                                    if re.match(r"\d{2}/\d{2}/\d{4}", ct):
                                        doc["uploaded"] = ct
                                        break
                                documents.append(doc)
            else:
                # Fallback: find all PDF/download links anywhere on page
                all_links = await page.query_selector_all("a[href]")
                for link in all_links:
                    href = await link.get_attribute("href") or ""
                    name = (await link.inner_text()).strip()
                    if name and len(name) > 2 and (
                        href.lower().endswith(".pdf")
                        or "/download" in href
                        or "/document" in href
                        or "getFile" in href
                    ):
                        if href.startswith("/"):
                            href = f"{self.BASE_URL}{href}"
                        documents.append({"name": name, "url": href})

        except Exception as e:
            print(f"  [DOC EXTRACT ERROR] section='{section_title}': {e}")

        return documents

    async def _save_snapshot(self, page: Page, prefix: str):
        """Save listing page HTML for debugging."""
        try:
            html = await page.content()
            snap_dir = os.path.join(self.STORAGE_DIR, "raw_snapshots")
            os.makedirs(snap_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(snap_dir, f"{prefix}_{ts}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  Snapshot: {path}")
        except Exception as e:
            print(f"  [SNAPSHOT WARNING] {e}")

    def _clean_listing_snapshots(self):
        """
        Delete old HTML snapshots ONLY.
        Does NOT touch PDFs, detail pages, or any other storage.
        Called at the start of each run.
        """
        snap_dir = os.path.join(self.STORAGE_DIR, "raw_snapshots")
        if not os.path.exists(snap_dir):
            return

        count = 0
        for fname in os.listdir(snap_dir):
            if fname.endswith(".html"):
                try:
                    os.remove(os.path.join(snap_dir, fname))
                    count += 1
                except Exception:
                    pass

        if count:
            print(f"  Cleaned {count} old HTML snapshots")
