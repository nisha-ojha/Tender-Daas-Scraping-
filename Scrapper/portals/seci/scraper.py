"""
portals/seci/scraper.py

Complete SECI scraper that handles:
1. Three separate pages (Live, Archive, Results — each has its own URL)
2. Pagination with loop detection (SECI uses DataTables <button> pagination)
3. Two-pass table extraction (fixes stale element issue)
4. Detail page scraping for each tender
5. HTML snapshots for debugging
"""

import asyncio
import json
import re
import os
import random
from datetime import datetime

from playwright.async_api import async_playwright

from core.db import insert_raw_record
from core.retry import retry_async
from portals.seci.config import (
    PORTAL_NAME, BASE_URL, PAGES_TO_SCRAPE,
    RATE_LIMIT_SECONDS, PAGE_TIMEOUT_MS,
    SELECTORS, DETAIL_SELECTORS, BROWSER_OPTIONS,
    MAX_PAGES_PER_URL,
)
from portals.seci.field_map import build_column_index


# ============================================================
# ENTRY POINT
# ============================================================

def scrape(conn, batch_id):
    """Main entry point — called by pipeline.py"""
    return asyncio.run(_scrape_async(conn, batch_id))


# ============================================================
# MAIN SCRAPER
# ============================================================

async def _scrape_async(conn, batch_id):

    total_saved = 0
    browser = None

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(
                headless=BROWSER_OPTIONS["headless"],
            )

            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )

            page = await context.new_page()

            # Loop through each configured page (live, archive, results)
            for page_config in PAGES_TO_SCRAPE:
                page_name = page_config["name"]
                page_url = page_config["url"]
                tender_status = page_config["tender_status"]

                print(f"\n━━━ {page_name.upper()} ({page_url}) ━━━")

                try:
                    async def load_page():
                        await page.goto(
                            page_url,
                            timeout=PAGE_TIMEOUT_MS,
                            wait_until="networkidle",
                        )

                    await retry_async(
                        load_page, max_retries=3, base_delay=5,
                        operation_name=f"{page_name} load",
                    )

                    await asyncio.sleep(4)
                    await _save_snapshot(page, f"seci_{tender_status}")

                    count = await _scrape_all_pages(
                        page, conn, batch_id, tender_status, page_url
                    )

                    total_saved += count
                    print(f"━━━ {page_name}: {count} records saved ━━━")

                except Exception as e:
                    print(f"[PAGE ERROR] {page_name}: {e}")
                    continue

        except Exception as e:
            print(f"[SCRAPER ERROR] {e}")
            raise
        finally:
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass

    print(f"\nScraping complete: {total_saved} total records")
    return total_saved


# ============================================================
# PAGINATION
# ============================================================

async def _scrape_all_pages(page, conn, batch_id, tender_status, page_url):
    """Scrape all pagination pages of the current URL."""

    total = 0
    page_num = 1

    while page_num <= MAX_PAGES_PER_URL:
        print(f"\n  Page {page_num}:")

        count = await _scrape_current_table(
            page, conn, batch_id, tender_status, page_url
        )

        total += count

        if count == 0:
            print(f"  No records on page {page_num}. Done.")
            break

        # Try to go to next page
        has_next = await _click_next_page(page)

        if not has_next:
            print(f"  No more pages.")
            break

        page_num += 1
        await asyncio.sleep(random.uniform(2.0, 3.5))

    return total


async def _click_next_page(page):
    """
    Click the 'Next' pagination button.
    SECI uses DataTables with <button> elements, NOT <a> links.

    Returns True if we navigated to a new page, False if no more pages.
    Detects infinite loops by comparing first row text before and after click.
    """
    try:
        # Get the first row's text BEFORE clicking Next
        first_row = await page.query_selector("table tbody tr:first-child")
        old_text = ""
        if first_row:
            old_text = await first_row.inner_text()

        # Find the Next button (SECI DataTables uses <button> not <a>)
        btn = await page.query_selector(
            "button.dt-paging-button.next:not(.disabled)"
        )

        if not btn:
            return False

        is_visible = await btn.is_visible()
        if not is_visible:
            return False

        # Check if the button has the 'disabled' class
        classes = await btn.get_attribute("class") or ""
        if "disabled" in classes:
            return False

        # Click the button
        await btn.click()
        await asyncio.sleep(2)

        # Get the first row's text AFTER clicking
        new_first_row = await page.query_selector("table tbody tr:first-child")
        new_text = ""
        if new_first_row:
            new_text = await new_first_row.inner_text()

        # Compare: if the text is the same, we're stuck on the same page
        if new_text and new_text != old_text:
            return True   # Page actually changed → continue
        else:
            return False  # Same content → we've reached the end

    except Exception:
        return False


# ============================================================
# TWO-PASS TABLE SCRAPING
# ============================================================

async def _scrape_current_table(page, conn, batch_id, tender_status, source_url):
    """
    Scrape all rows from the currently visible table.

    TWO-PASS approach:
      Pass 1: Collect ALL listing data while rows are in the DOM
              (do NOT navigate away — keeps row references valid)
      Pass 2: Visit each detail page and save to database
              (safe to navigate because we already have all listing data)
    """

    saved = 0

    # Find table rows
    rows = await page.query_selector_all("table tbody tr")
    if not rows:
        rows = await page.query_selector_all("table tr")
    if not rows:
        print("    No rows found.")
        return 0

    # Build column map from headers
    col_index = {}
    headers = await page.query_selector_all("table thead th")
    if headers:
        header_texts = [(await h.inner_text()).strip() for h in headers]
        col_index = build_column_index(header_texts)
        print(f"    Headers: {header_texts}")
        print(f"    Column map: {col_index}")

    # ════════════════════════════════════════════════
    # PASS 1: Collect ALL listing data
    # (Do NOT navigate away during this loop)
    # ════════════════════════════════════════════════
    all_row_data = []

    for i, row in enumerate(rows):
        try:
            cells = await row.query_selector_all("td")
            if not cells or len(cells) < 2:
                continue

            cell_texts = [(await c.inner_text()).strip() for c in cells]

            full_text = " ".join(cell_texts)
            if len(full_text) < 10:
                continue

            # Build raw record from listing data
            raw_record = {
                "row_index": i,
                "full_text": full_text[:1000],
                "cell_texts": cell_texts,
                "cell_count": len(cell_texts),
                "tender_status": tender_status,
                "source_url": source_url,
                "scraped_at": datetime.utcnow().isoformat(),
            }

            # Map columns by name (dynamic, not hardcoded)
            for field_name, col_idx in col_index.items():
                if col_idx < len(cell_texts):
                    raw_record[field_name] = cell_texts[col_idx]

            # Fallback: use longest cell as title if no mapping found
            if "title" not in raw_record and cell_texts:
                longest = max(cell_texts, key=len)
                if len(longest) > 15:
                    raw_record["title"] = longest[:500]

            # Handle multi-line reference number
            # (SECI puts "Tender Ref No." label on line 1, actual ref on line 2)
            if "reference_number" in raw_record:
                ref = raw_record["reference_number"]
                if "\n" in ref:
                    lines = [l.strip() for l in ref.split("\n") if l.strip()]
                    raw_record["reference_number"] = lines[-1] if lines else ref

            # Find detail page URL
            detail_url = None
            links = await row.query_selector_all("a")
            for link in links:
                href = await link.get_attribute("href")
                if href and ("tender-details" in href or "tenderdetails" in href):
                    if href.startswith("/"):
                        href = f"{BASE_URL}{href}"
                    detail_url = href
                    break

            raw_record["detail_url"] = detail_url
            all_row_data.append(raw_record)

        except Exception as e:
            print(f"    [ROW ERROR] Row {i} (pass 1): {e}")
            continue

    print(f"    Collected {len(all_row_data)} rows. Visiting detail pages...")

    # ════════════════════════════════════════════════
    # PASS 2: Visit each detail page and save to DB
    # (Safe to navigate — we already have all listing data)
    # ════════════════════════════════════════════════

    for idx, raw_record in enumerate(all_row_data):
        try:
            detail_url = raw_record.get("detail_url")

            # Visit detail page if link exists
            if detail_url:
                detail_data = await _scrape_detail_page(page, detail_url, source_url)
                if detail_data:
                    raw_record["detail"] = detail_data

                # Polite delay between detail page visits
                await asyncio.sleep(random.uniform(
                    RATE_LIMIT_SECONDS,
                    RATE_LIMIT_SECONDS + 1.5,
                ))

            # Save to database
            record_id = insert_raw_record(
                conn=conn,
                portal=PORTAL_NAME,
                raw_data=raw_record,
                batch_id=batch_id,
            )

            if record_id:
                saved += 1
                title = raw_record.get("title", raw_record.get("full_text", ""))[:70]
                status_tag = tender_status.upper()
                has_detail = "+" if "detail" in raw_record else "-"
                print(f"    #{saved} [{status_tag}][{has_detail}]: {title}...")

        except Exception as e:
            print(f"    [ROW ERROR] Row {idx} (pass 2): {e}")
            continue

    return saved


# ============================================================
# DETAIL PAGE EXTRACTION
# ============================================================

async def _scrape_detail_page(page, detail_url, return_url):
    """
    Navigate to a tender's detail page, extract data, then go back.

    Args:
        page: Playwright page object
        detail_url: URL of the detail page (e.g. /tender-details/YmZ3)
        return_url: URL to return to (the listing page we came from)

    Returns:
        Dictionary with extracted detail data, or None on failure
    """
    detail = {}

    try:
        # Navigate to detail page with retry
        async def goto_detail():
            await page.goto(
                detail_url,
                timeout=PAGE_TIMEOUT_MS,
                wait_until="networkidle",
            )

        await retry_async(
            goto_detail, max_retries=2, base_delay=3,
            operation_name="Detail page",
        )
        await asyncio.sleep(2)

        # ── Extract all label-value pairs from tables ──
        all_rows = await page.query_selector_all("table tr")

        for row in all_rows:
            cells = await row.query_selector_all("td, th")
            cell_texts = []
            for cell in cells:
                text = await cell.inner_text()
                cell_texts.append(text.strip())

            # Labels and values alternate: [label, value, label, value, ...]
            for j in range(0, len(cell_texts) - 1, 2):
                label = cell_texts[j]
                value = cell_texts[j + 1] if j + 1 < len(cell_texts) else ""

                if label and value and 3 < len(label) < 100:
                    label = label.rstrip(":").strip()
                    detail[label] = value

        # ── Extract document links ──
        detail["documents"] = await _extract_documents(page, "Tender Documents")
        detail["announcements"] = await _extract_documents(page, "Tender Related Announcements")

    except Exception as e:
        detail["_error"] = str(e)[:200]
        print(f"      [DETAIL ERROR] {str(e)[:100]}")

    finally:
        # ── Navigate back to listing page ──
        try:
            await page.go_back(timeout=30000, wait_until="networkidle")
            await asyncio.sleep(2)

            # Verify we're back on the right page
            current_url = page.url
            if "tenders" not in current_url.lower():
                # go_back didn't work — navigate directly
                await page.goto(
                    return_url,
                    timeout=PAGE_TIMEOUT_MS,
                    wait_until="networkidle",
                )
                await asyncio.sleep(3)

        except Exception:
            # Last resort — navigate directly to the listing page
            try:
                await page.goto(
                    return_url,
                    timeout=PAGE_TIMEOUT_MS,
                    wait_until="networkidle",
                )
                await asyncio.sleep(3)
            except Exception as e2:
                print(f"      [NAV ERROR] Could not return to listing: {e2}")

    return detail if detail else None


# ============================================================
# DOCUMENT EXTRACTION
# ============================================================

async def _extract_documents(page, section_title):
    """
    Extract document links from a named section on the detail page.
    (e.g., "Tender Documents" or "Tender Related Announcements")

    Returns list: [{"name": "file.pdf", "url": "...", "uploaded": "10/03/2026"}, ...]
    """
    documents = []

    try:
        # Find the section header, then get its parent table
        headers = await page.query_selector_all("th, td, h3, h4")
        target_table = None

        for header in headers:
            text = await header.inner_text()
            if section_title.lower() in text.lower():
                # Use JavaScript to find the closest table
                target_table = await header.evaluate_handle("""
                    el => {
                        let table = el.closest('table');
                        if (table) return table;
                        let next = el.nextElementSibling;
                        while (next) {
                            if (next.tagName === 'TABLE') return next;
                            let t = next.querySelector('table');
                            if (t) return t;
                            next = next.nextElementSibling;
                        }
                        return null;
                    }
                """)
                break

        if target_table:
            rows = await target_table.query_selector_all("tr")
            for row in rows:
                links = await row.query_selector_all("a")
                cells = await row.query_selector_all("td")

                for link in links:
                    href = await link.get_attribute("href")
                    name = await link.inner_text()

                    if href and name and len(name.strip()) > 2:
                        if href.startswith("/"):
                            href = f"{BASE_URL}{href}"

                        doc = {"name": name.strip(), "url": href}

                        # Try to find upload date in row cells
                        for cell in cells:
                            ct = await cell.inner_text()
                            ct = ct.strip()
                            if re.match(r"\d{2}/\d{2}/\d{4}", ct):
                                doc["uploaded"] = ct
                                break

                        documents.append(doc)

    except Exception as e:
        print(f"      [DOC ERROR] {section_title}: {e}")

    return documents


# ============================================================
# SNAPSHOT UTILITY
# ============================================================

async def _save_snapshot(page, prefix):
    """Save current page HTML as a snapshot file for debugging."""
    try:
        html = await page.content()
        snapshot_dir = os.path.join("storage", "raw_snapshots")
        os.makedirs(snapshot_dir, exist_ok=True)
        filename = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        filepath = os.path.join(snapshot_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"  Snapshot: {filepath} ({len(html):,} chars)")
    except Exception as e:
        print(f"  [SNAPSHOT WARNING] {e}")