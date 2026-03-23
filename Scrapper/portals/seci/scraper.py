"""
portals/seci/scraper.py
-----------------------
Scrapes tenders from seci.co.in/tenders

This scraper:
  1. Opens the SECI tenders page in a headless browser
  2. Reads the header row to build a dynamic column map
  3. Extracts every tender row into a raw_data dictionary
  4. Saves each record to raw_records table
  5. Saves an HTML snapshot for debugging

The scraper does NOT clean data — that's the normalizer's job.
It just captures everything as-is from the website.
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
    PORTAL_NAME, BASE_URL, TENDERS_URL,
    RATE_LIMIT_SECONDS, PAGE_TIMEOUT_MS, TABLE_WAIT_MS,
    SELECTORS, BROWSER_OPTIONS,
)
from portals.seci.field_map import build_column_index, validate_column_index


def scrape(conn, batch_id):
    """
    Main entry point — called by pipeline.py

    Args:
        conn: Database connection (transaction managed by pipeline)
        batch_id: Unique ID for this run

    Returns:
        Number of raw records saved
    """
    # asyncio.run() bridges sync (pipeline) → async (playwright)
    return asyncio.run(_scrape_async(conn, batch_id))


async def _scrape_async(conn, batch_id):
    """The actual async scraping logic."""

    records_saved = 0

    async with async_playwright() as pw:
        # ── Launch browser ──
        browser = await pw.chromium.launch(
            headless=BROWSER_OPTIONS["headless"],
        )

        # Create a browser context with realistic settings
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )

        page = await context.new_page()

        try:
            # ── Load the tenders page (with retry) ──
            print(f"  Loading {TENDERS_URL}...")

            async def load_page():
                await page.goto(TENDERS_URL, timeout=PAGE_TIMEOUT_MS)
                # Wait for the table to appear
                await page.wait_for_selector(
                    SELECTORS["tender_table"],
                    timeout=TABLE_WAIT_MS,
                )

            await retry_async(
                load_page,
                max_retries=3,
                base_delay=3,
                operation_name="SECI page load",
            )

            # Small random delay (act human)
            await asyncio.sleep(random.uniform(1.0, 2.0))

            # ── Save HTML snapshot ──
            html = await page.content()
            snapshot_dir = os.path.join("storage", "raw_snapshots")
            os.makedirs(snapshot_dir, exist_ok=True)
            snapshot_file = os.path.join(
                snapshot_dir,
                f"seci_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
            )
            with open(snapshot_file, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  Saved HTML snapshot: {snapshot_file}")

            # ── Read table headers → build column map ──
            header_elements = await page.query_selector_all(SELECTORS["table_headers"])

            if header_elements:
                header_texts = []
                for h in header_elements:
                    text = await h.inner_text()
                    header_texts.append(text.strip())
                print(f"  Table headers found: {header_texts}")

                col_index = build_column_index(header_texts)
                is_valid, missing = validate_column_index(col_index)
                print(f"  Column map: {col_index}")

                if not is_valid:
                    raise ValueError(
                        f"Critical columns missing from SECI table: {missing}. "
                        f"Website structure may have changed. Check HTML snapshot."
                    )
            else:
                print("  WARNING: No <th> headers found. Using position-based fallback.")
                col_index = {}

            # ── Extract all tender rows ──
            rows = await page.query_selector_all(SELECTORS["tender_table"])

            if not rows:
                # Try fallback selectors
                rows = await page.query_selector_all(SELECTORS["fallback_items"])

            if not rows:
                print("  WARNING: No tender rows found! Website may have changed.")
                print(f"  Check snapshot: {snapshot_file}")
                return 0

            print(f"  Found {len(rows)} table rows")

            # ── Process each row ──
            for i, row in enumerate(rows):
                try:
                    # Get all cells in this row
                    cells = await row.query_selector_all("td")
                    if not cells or len(cells) < 2:
                        continue  # Skip empty/header rows

                    # Extract text from all cells
                    cell_texts = []
                    for cell in cells:
                        text = await cell.inner_text()
                        cell_texts.append(text.strip())

                    # Skip if row is too short to be a real tender
                    full_text = " ".join(cell_texts)
                    if len(full_text) < 10:
                        continue

                    # ── Build raw record using column map ──
                    raw_record = {
                        "row_index": i,
                        "full_text": full_text[:1000],
                        "cell_texts": cell_texts,
                        "cell_count": len(cell_texts),
                        "source_url": TENDERS_URL,
                        "scraped_at": datetime.utcnow().isoformat(),
                    }

                    # Map known columns by name (not position!)
                    for field_name, col_idx in col_index.items():
                        if col_idx < len(cell_texts):
                            raw_record[field_name] = cell_texts[col_idx]

                    # If no column map, fall back to positional extraction
                    if not col_index and len(cell_texts) >= 3:
                        raw_record["title"] = cell_texts[0] if len(cell_texts[0]) > 20 else full_text[:300]

                    # ── Extract links (document URLs, detail page) ──
                    links = await row.query_selector_all("a")
                    doc_urls = []
                    detail_url = None

                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            # Make absolute URL if relative
                            if href.startswith("/"):
                                href = f"{BASE_URL}{href}"

                            # Classify the link
                            if "tender-details" in href or "tenderdetails" in href:
                                detail_url = href
                            else:
                                doc_urls.append(href)

                    raw_record["detail_url"] = detail_url
                    raw_record["document_urls"] = doc_urls

                    # ── Handle multi-line cells ──
                    # SECI's "Tender Ref No" cell has the label on line 1
                    # and the actual ref number on line 2
                    if "reference_number" in raw_record:
                        ref = raw_record["reference_number"]
                        if "\n" in ref:
                            # Take the last non-empty line
                            lines = [l.strip() for l in ref.split("\n") if l.strip()]
                            raw_record["reference_number"] = lines[-1] if lines else ref

                    # ── Save to database ──
                    record_id = insert_raw_record(
                        conn=conn,
                        portal=PORTAL_NAME,
                        raw_data=raw_record,
                        batch_id=batch_id,
                    )

                    if record_id:
                        records_saved += 1
                        title_preview = raw_record.get("title", full_text)[:80]
                        print(f"    #{records_saved}: {title_preview}...")

                except Exception as e:
                    print(f"    [ERROR] Row {i}: {e}")
                    continue  # Skip bad rows, don't crash

                # Polite delay between processing rows
                # (not strictly needed for table extraction, but good habit)
                if i > 0 and i % 10 == 0:
                    await asyncio.sleep(random.uniform(0.5, 1.0))

        except Exception as e:
            print(f"  [SCRAPER ERROR] {e}")
            raise  # Let pipeline.py handle rollback

        finally:
            await browser.close()

    print(f"  Scraping complete: {records_saved} records saved")
    return records_saved
