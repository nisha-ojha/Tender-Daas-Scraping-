"""
core/pipeline.py
----------------
The engine that runs each stage of the pipeline in order.

HOW IT WORKS:
  1. Opens a database transaction (all-or-nothing envelope)
  2. Runs Stage 1: Scrape → saves raw records
  3. Runs Stage 2: Normalize → cleans data into tenders table
  4. Runs Stage 3: Deduplicate (placeholder — Week 3)
  5. If ALL stages pass → COMMIT (save everything)
  6. If ANY stage fails → ROLLBACK (undo everything)

ADDING A NEW PORTAL:
  Just create portals/new_portal/scraper.py and normalizer.py
  Then run: python main.py --portal new_portal
  importlib auto-discovers the portal — no changes needed here.
"""

import importlib
import traceback
from datetime import datetime

from core.db import get_connection, log_scraper_run
from core.alerts import alert_success, alert_error, alert_info


def run_pipeline(portal: str, batch_id: str, stages: str = "all") -> dict:
    """
    Run the full scraping pipeline for a given portal.

    Args:
        portal:   Name of the portal ('seci', 'cppp', etc.)
                  Must match a folder name under portals/
        batch_id: Unique ID for this run (for tracking & rollback)
        stages:   Which stages to run ('all', 'scrape', 'normalize', 'dedup')

    Returns:
        dict: {"new": int, "updated": int, "errors": int, "raw_records": int}

    Raises:
        Exception if any stage fails (after rollback is done)
    """
    print(f"\n{'='*60}")
    print(f"  PIPELINE START: {portal.upper()}")
    print(f"  Batch:  {batch_id}")
    print(f"  Stages: {stages}")
    print(f"  Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    result = {"new": 0, "updated": 0, "errors": 0, "raw_records": 0}

    # ── Load portal modules ───────────────────────────────────────────
    # importlib.import_module dynamically loads portals/seci/scraper.py etc.
    # When you add portals/cppp/scraper.py, this discovers it automatically.
    try:
        scraper_mod = importlib.import_module(f"portals.{portal}.scraper")
        normalizer_mod = importlib.import_module(f"portals.{portal}.normalizer")
    except ModuleNotFoundError as e:
        msg = (
            f"Portal '{portal}' not found. "
            f"Make sure portals/{portal}/scraper.py and normalizer.py exist.\n"
            f"Error: {e}"
        )
        print(f"  [ERROR] {msg}")
        raise ModuleNotFoundError(msg)

    # ── Open database transaction ─────────────────────────────────────
    conn = get_connection()

    try:
        conn.autocommit = False  # All stages share one transaction

        # ── Stage 1: SCRAPE ──────────────────────────────────────────
        if stages in ("all", "scrape"):
            print(f"  ── Stage 1: SCRAPE ({portal.upper()}) ──")
            raw_count = scraper_mod.scrape(conn=conn, batch_id=batch_id)
            result["raw_records"] = raw_count
            print(f"  ── Stage 1 DONE: {raw_count} raw records saved ──\n")

        # ── Stage 2: NORMALIZE ───────────────────────────────────────
        if stages in ("all", "normalize"):
            print(f"  ── Stage 2: NORMALIZE ({portal.upper()}) ──")
            norm_result = normalizer_mod.normalize(conn=conn, batch_id=batch_id)
            result["new"] = norm_result.get("new", 0)
            result["errors"] = norm_result.get("errors", 0)
            print(f"  ── Stage 2 DONE: {result['new']} new tenders ──\n")

        # ── Stage 3: DEDUPLICATE ──────────────────────────────────────
        if stages in ("all", "dedup"):
            print(f"  ── Stage 3: DEDUPLICATE (not built yet — skipping) ──\n")

        # ── ALL STAGES PASSED → COMMIT ────────────────────────────────
        conn.commit()
        print(f"  ✓ COMMITTED: All changes saved to database.")

        # Log the successful run using a FRESH connection.
        # Reason: conn is closed in the finally block below. If we pass conn
        # here, log_scraper_run() would crash on a closed connection.
        log_conn = get_connection()
        try:
            log_scraper_run(
                conn=log_conn,        # ← fresh conn, NOT the transaction conn
                portal=portal,
                batch_id=batch_id,
                status="success",
                records_found=result["raw_records"],
                records_new=result["new"],
                records_updated=result["updated"],
            )
        finally:
            log_conn.close()

        return result

    except Exception as e:
        # ── ANY STAGE FAILED → ROLLBACK ───────────────────────────────
        conn.rollback()
        error_detail = traceback.format_exc()
        print(f"\n  ✗ ROLLED BACK: All changes undone.")
        print(f"  Error: {e}")
        print(f"  Traceback:\n{error_detail}")

        # Log the failed run independently (its own connection + commit)
        try:
            log_conn = get_connection()
            log_scraper_run(
                conn=log_conn,
                portal=portal,
                batch_id=batch_id,
                status="error",
                records_found=result["raw_records"],
                error_message=str(e)[:500],
            )
            log_conn.close()
        except Exception:
            print("  [WARNING] Could not log failed run to database")

        raise  # Re-raise so main.py / scheduler.py can send the alert

    finally:
        conn.close()
        print(f"\n{'='*60}")
        print(f"  PIPELINE END: {portal.upper()}")
        print(f"{'='*60}\n")
