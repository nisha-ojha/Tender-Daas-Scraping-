"""
portals/seci/scraper.py

TWO-PHASE ARCHITECTURE:
  Phase 1 (FAST): Pagination — collect all listing data from all pages
                  NO detail page visits during this phase
                  DataTable stays intact, pagination works correctly

  Phase 2 (SLOW): Detail pages — visit each tender's detail page
                  Extract EMD, dates, documents, full HTML
                  Database check: skip if reference_number already exists
"""

import asyncio
import json
import re
import os
import shutil
import random
from datetime import datetime

from playwright.async_api import async_playwright

from core.db import insert_raw_record, find_by_reference
from core.retry import retry_async
from portals.seci.config import (
    PORTAL_NAME, PORTAL_SHORT, BASE_URL, PAGES_TO_SCRAPE,
    RATE_LIMIT_SECONDS, PAGE_TIMEOUT_MS,
    BROWSER_OPTIONS, TEST_MAX_PAGES,
)
from portals.seci.field_map import build_column_index


# ============================================================
# ENTRY POINT
# ============================================================

def scrape(conn, batch_id):
    """Called by pipeline.py"""
    # Step 0: Clean old snapshots before starting
    _clean_storage()
    return asyncio.run(_scrape_async(conn, batch_id))


# ============================================================
# MAIN SCRAPER
# ============================================================

async def _scrape_async(conn, batch_id):

    total_saved = 0
    browser = None

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(headless=BROWSER_OPTIONS["headless"])
            context = await browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
            )
            page = await context.new_page()

            for page_config in PAGES_TO_SCRAPE:
                page_name = page_config["name"]
                page_url = page_config["url"]
                tender_status = page_config["tender_status"]

                print(f"\n{'━'*60}")
                print(f"  {page_name.upper()} ({page_url})")
                print(f"{'━'*60}")

                try:
                    count = await _scrape_one_portal(
                        page, conn, batch_id, page_url, tender_status, page_name
                    )
                    total_saved += count
                    print(f"\n  ✓ {page_name}: {count} records saved")

                except Exception as e:
                    print(f"\n  ✗ {page_name} FAILED: {e}")
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

    print(f"\n{'='*60}")
    print(f"  TOTAL: {total_saved} records saved")
    print(f"{'='*60}")
    return total_saved


async def _scrape_one_portal(page, conn, batch_id, page_url, tender_status, page_name):
    """
    Scrape one portal URL (e.g. /tenders/archive) using two phases.
    """

    # ══════════════════════════════════════════════════════════
    # PHASE 1: PAGINATION — Collect all listing data
    # No detail page visits. DataTable stays intact.
    # ══════════════════════════════════════════════════════════

    print(f"\n  ── PHASE 1: Collecting listing data ──")

    # Load the page
    async def load_page():
        await page.goto(page_url, timeout=PAGE_TIMEOUT_MS, wait_until="networkidle")

    await retry_async(load_page, max_retries=3, base_delay=5, operation_name=f"{page_name} load")
    await asyncio.sleep(3)

    # Save one snapshot per portal URL
    await _save_snapshot(page, f"seci_{tender_status}_listing")

    all_rows = []   # Collect everything here
    page_num = 1
    max_pages = TEST_MAX_PAGES if TEST_MAX_PAGES > 0 else 999

    while page_num <= max_pages:
        print(f"\n    Page {page_num}:")

        # Extract rows from current page
        rows_on_page = await _extract_listing_rows(page, tender_status, page_url)
        print(f"      Collected {len(rows_on_page)} rows")

        if not rows_on_page:
            print(f"      No rows found. Done with pagination.")
            break

        all_rows.extend(rows_on_page)

        # Try next page
        has_next = await _click_next_page(page, page_num)
        if not has_next:
            print(f"      Last page reached.")
            break

        page_num += 1
        await asyncio.sleep(random.uniform(1.5, 2.5))

    print(f"\n  Phase 1 complete: {len(all_rows)} total rows from {page_num} pages")

    if not all_rows:
        return 0

    # ══════════════════════════════════════════════════════════
    # PHASE 2: DETAIL PAGES — Visit each tender one by one
    # Check DB first: skip detail visit if tender already exists
    # ══════════════════════════════════════════════════════════

    print(f"\n  ── PHASE 2: Visiting detail pages ──")

    saved = 0
    skipped_existing = 0

    for idx, raw_record in enumerate(all_rows):
        ref = raw_record.get("reference_number", "")
        detail_url = raw_record.get("detail_url")
        title_short = raw_record.get("title", "")[:60]

        # Check if this tender already exists in DB
        if ref:
            existing = find_by_reference(conn, ref, PORTAL_SHORT)
            if existing:
                skipped_existing += 1
                # Still save to raw_records (listing data only, no detail visit)
                record_id = insert_raw_record(
                    conn=conn, portal=PORTAL_NAME,
                    raw_data=raw_record, batch_id=batch_id,
                )
                if record_id:
                    saved += 1
                print(f"    #{idx+1} [SKIP] {ref} — already in DB")
                continue

        # Visit detail page for NEW tenders
        if detail_url:
            print(f"    #{idx+1} [NEW] Visiting detail: {title_short}...")

            detail_data, detail_html = await _scrape_detail_page(page, detail_url)

            if detail_data:
                raw_record["detail"] = detail_data

            # Save detail page HTML
            if detail_html:
                _save_detail_html(detail_html, raw_record.get("seci_tender_id", f"row_{idx}"))

            await asyncio.sleep(random.uniform(RATE_LIMIT_SECONDS, RATE_LIMIT_SECONDS + 1.5))
        else:
            print(f"    #{idx+1} [NEW] No detail URL: {title_short}")

        # Save to database
        record_id = insert_raw_record(
            conn=conn, portal=PORTAL_NAME,
            raw_data=raw_record, batch_id=batch_id,
        )
        if record_id:
            saved += 1

    print(f"\n  Phase 2 complete: {saved} saved, {skipped_existing} skipped (already in DB)")
    return saved


# ============================================================
# PHASE 1: LISTING DATA EXTRACTION (one page, no navigation)
# ============================================================

async def _extract_listing_rows(page, tender_status, source_url):
    """Extract all rows from the currently visible table. NO navigation."""

    rows = await page.query_selector_all("table tbody tr")
    if not rows:
        return []

    # Build column map
    col_index = {}
    headers = await page.query_selector_all("table thead th")
    if headers:
        header_texts = [(await h.inner_text()).strip() for h in headers]
        col_index = build_column_index(header_texts)

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
                "full_text": full_text[:1000],
                "cell_texts": cell_texts,
                "cell_count": len(cell_texts),
                "tender_status": tender_status,
                "source_url": source_url,
                "scraped_at": datetime.utcnow().isoformat(),
            }

            # Map columns dynamically
            for field_name, col_idx in col_index.items():
                if col_idx < len(cell_texts):
                    raw_record[field_name] = cell_texts[col_idx]

            # Fallback title
            if "title" not in raw_record and cell_texts:
                longest = max(cell_texts, key=len)
                if len(longest) > 15:
                    raw_record["title"] = longest[:500]

            # Clean multi-line reference number
            if "reference_number" in raw_record:
                ref = raw_record["reference_number"]
                if "\n" in ref:
                    lines = [l.strip() for l in ref.split("\n") if l.strip()]
                    raw_record["reference_number"] = lines[-1] if lines else ref

            # Extract detail URL (but DON'T navigate to it)
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

            # Extract ALL links from the row (documents, etc.)
            all_links = []
            for link in links:
                href = await link.get_attribute("href")
                text = await link.inner_text()
                if href:
                    if href.startswith("/"):
                        href = f"{BASE_URL}{href}"
                    all_links.append({"text": text.strip(), "url": href})
            raw_record["listing_links"] = all_links

            extracted.append(raw_record)

        except Exception as e:
            print(f"      [ROW ERROR] Row {i}: {e}")
            continue

    return extracted


# ============================================================
# PAGINATION (Phase 1 only — no detail visits happen here)
# ============================================================

async def _click_next_page(page, current_page_num):
    """
    Click the Next button on SECI DataTable.
    Uses page number comparison instead of row text comparison.
    """
    try:
        # Method 1: Check the "Showing X to Y of Z entries" text
        info_el = await page.query_selector(".dt-info, #tender-list_info")
        if info_el:
            info_text = await info_el.inner_text()
            # Parse "Showing 1 to 10 of 229 entries"
            match = re.search(r"Showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)", info_text)
            if match:
                end_row = int(match.group(2))
                total_rows = int(match.group(3))
                print(f"      Pagination: showing up to {end_row} of {total_rows}")
                if end_row >= total_rows:
                    return False  # We've seen all rows

        # Find and click the Next button
        btn = await page.query_selector(
            "button.dt-paging-button.next:not(.disabled)"
        )

        if not btn:
            return False

        # Check if disabled
        classes = await btn.get_attribute("class") or ""
        if "disabled" in classes:
            return False

        is_visible = await btn.is_visible()
        if not is_visible:
            return False

        await btn.click()
        await asyncio.sleep(2)

        # Verify the info text changed
        if info_el:
            new_info = await info_el.inner_text()
            new_match = re.search(r"Showing\s+(\d+)\s+to\s+(\d+)", new_info)
            if new_match and match:
                new_start = int(new_match.group(1))
                old_start = int(match.group(1))
                if new_start == old_start:
                    return False  # Didn't actually move

        return True

    except Exception:
        return False


# ============================================================
# PHASE 2: DETAIL PAGE EXTRACTION
# ============================================================

async def _scrape_detail_page(page, detail_url):
    """
    Navigate to detail page, extract everything, come back.

    Returns:
        (detail_dict, html_string) — detail data and full page HTML
    """
    detail = {}
    html_content = None

    try:
        async def goto():
            await page.goto(detail_url, timeout=PAGE_TIMEOUT_MS, wait_until="networkidle")

        await retry_async(goto, max_retries=2, base_delay=3, operation_name="Detail page")
        await asyncio.sleep(2)

        # Capture full HTML of detail page
        html_content = await page.content()

        # ── Extract all label-value pairs ──
        all_rows = await page.query_selector_all("table tr")
        for row in all_rows:
            cells = await row.query_selector_all("td, th")
            texts = [(await c.inner_text()).strip() for c in cells]

            for j in range(0, len(texts) - 1, 2):
                label = texts[j].rstrip(":").strip()
                value = texts[j + 1]
                if label and value and 3 < len(label) < 100:
                    detail[label] = value

        # ── Extract ALL links on the detail page ──
        all_links = await page.query_selector_all("a[href]")
        page_links = []
        for link in all_links:
            try:
                href = await link.get_attribute("href")
                text = await link.inner_text()
                if href and text:
                    text = text.strip()
                    if href.startswith("/"):
                        href = f"{BASE_URL}{href}"
                    if len(text) > 1 and href.startswith("http"):
                        page_links.append({"text": text, "url": href})
            except Exception:
                continue
        detail["all_links"] = page_links

        # ── Extract document links specifically ──
        detail["documents"] = await _extract_documents(page, "Tender Documents")
        detail["announcements"] = await _extract_documents(page, "Tender Related Announcements")

        # Count total docs
        doc_count = len(detail.get("documents", [])) + len(detail.get("announcements", []))
        if doc_count > 0:
            print(f"      Found {doc_count} documents")

    except Exception as e:
        detail["_error"] = str(e)[:200]
        print(f"      [DETAIL ERROR] {str(e)[:80]}")

    finally:
        # Go back — use browser back, not page.goto (avoids DNS issues)
        try:
            await page.go_back(timeout=30000, wait_until="networkidle")
            await asyncio.sleep(2)
        except Exception:
            pass  # We don't need to go back since Phase 2 doesn't use the listing page

    return detail, html_content


async def _extract_documents(page, section_title):
    """Extract document links from a section like 'Tender Documents'."""
    documents = []
    try:
        headers = await page.query_selector_all("th, td, h3, h4")
        target_table = None

        for header in headers:
            text = await header.inner_text()
            if section_title.lower() in text.lower():
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

                        # Find upload date
                        for cell in cells:
                            ct = (await cell.inner_text()).strip()
                            if re.match(r"\d{2}/\d{2}/\d{4}", ct):
                                doc["uploaded"] = ct
                                break

                        documents.append(doc)

    except Exception as e:
        print(f"      [DOC ERROR] {section_title}: {e}")

    return documents


# ============================================================
# STORAGE & CLEANUP
# ============================================================

def _clean_storage():
    """Delete old HTML snapshots before a new run."""
    snapshot_dir = os.path.join("storage", "raw_snapshots")
    if os.path.exists(snapshot_dir):
        count = 0
        for f in os.listdir(snapshot_dir):
            if f.endswith(".html"):
                os.remove(os.path.join(snapshot_dir, f))
                count += 1
        if count > 0:
            print(f"  Cleaned {count} old HTML snapshots")

    # Also clean detail page HTMLs
    detail_dir = os.path.join("storage", "detail_pages")
    if os.path.exists(detail_dir):
        shutil.rmtree(detail_dir)
        print(f"  Cleaned old detail page HTMLs")


async def _save_snapshot(page, prefix):
    """Save listing page HTML snapshot."""
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


def _save_detail_html(html_content, tender_id):
    """Save detail page HTML for debugging/validation."""
    try:
        detail_dir = os.path.join("storage", "detail_pages")
        os.makedirs(detail_dir, exist_ok=True)
        # Clean tender_id for filename
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", str(tender_id))
        filepath = os.path.join(detail_dir, f"{safe_id}.html")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)
    except Exception:
        pass  # Don't crash if saving HTML fails