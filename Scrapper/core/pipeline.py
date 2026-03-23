"""
core/pipeline.py
----------------
The engine that runs each stage of the pipeline in order.

HOW IT WORKS:
  1. Opens a database transaction (all-or-nothing envelope)
  2. Runs Stage 1: Scrape → saves raw records
  3. Runs Stage 2: Normalize → cleans data into tenders table
  4. Runs Stage 3: Deduplicate (skipped for now)
  5. If ALL stages pass → COMMIT (save everything)
  6. If ANY stage fails → ROLLBACK (undo everything) + send alert

ADDING A NEW PORTAL:
  Just create portals/new_portal/scraper.py and normalizer.py
  Then run: python main.py --portal new_portal
  This file auto-discovers the portal module using importlib.
"""

import importlib
import traceback
from datetime import datetime

from core.db import get_connection, log_scraper_run
from core.alerts import alert_success, alert_error, alert_info


def run_pipeline(portal, batch_id, stages="all"):
    """
    Run the full scraping pipeline for a given portal.

    Args:
        portal: Name of the portal ('seci', 'cppp', etc.)
                Must match a folder name under portals/
        batch_id: Unique ID for this run (for tracking & rollback)
        stages: Which stages to run ('all', 'scrape', 'normalize', 'dedup')

    Returns:
        Dictionary with counts: {"new": X, "updated": Y, "errors": Z}

    Raises:
        Exception if any stage fails (after rollback is done)
    """
    print(f"\n{'='*60}")
    print(f"  PIPELINE START: {portal.upper()}")
    print(f"  Batch: {batch_id}")
    print(f"  Stages: {stages}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    result = {"new": 0, "updated": 0, "errors": 0, "raw_records": 0}

    # ── Step 1: Load the portal's modules ──
    # This is where the magic happens: importlib lets us load
    # portals/seci/scraper.py or portals/cppp/scraper.py
    # based on the portal name passed in from main.py
    try:
        scraper_mod = importlib.import_module(f"portals.{portal}.scraper")
        normalizer_mod = importlib.import_module(f"portals.{portal}.normalizer")
    except ModuleNotFoundError as e:
        error_msg = (
            f"Portal '{portal}' not found. "
            f"Make sure portals/{portal}/scraper.py and normalizer.py exist.\n"
            f"Error: {e}"
        )
        print(f"  [ERROR] {error_msg}")
        raise ModuleNotFoundError(error_msg)

    # ── Step 2: Open database transaction ──
    conn = get_connection()

    try:
        # Disable autocommit = enable manual transaction control
        conn.autocommit = False

        # ── Stage 1: SCRAPE ──
        if stages in ("all", "scrape"):
            print(f"  ── Stage 1: SCRAPE ({portal.upper()}) ──")
            raw_count = scraper_mod.scrape(conn=conn, batch_id=batch_id)
            result["raw_records"] = raw_count
            print(f"  ── Stage 1 DONE: {raw_count} raw records saved ──\n")

        # ── Stage 2: NORMALIZE ──
        if stages in ("all", "normalize"):
            print(f"  ── Stage 2: NORMALIZE ({portal.upper()}) ──")
            norm_result = normalizer_mod.normalize(conn=conn, batch_id=batch_id)
            result["new"] = norm_result.get("new", 0)
            result["errors"] = norm_result.get("errors", 0)
            print(f"  ── Stage 2 DONE: {result['new']} new tenders ──\n")

        # ── Stage 3: DEDUPLICATE ──
        # TODO: Build this in Week 3
        if stages in ("all", "dedup"):
            print(f"  ── Stage 3: DEDUPLICATE (skipped — not built yet) ──\n")

        # ── ALL STAGES PASSED → COMMIT ──
        conn.commit()
        print(f"  ✓ COMMITTED: All changes saved to database.")

        # Log successful run
        log_scraper_run(
            conn=conn,
            portal=portal,
            batch_id=batch_id,
            status="success",
            records_found=result["raw_records"],
            records_new=result["new"],
            records_updated=result["updated"],
        )

        return result

    except Exception as e:
        # ── ANY STAGE FAILED → ROLLBACK ──
        conn.rollback()

        error_detail = traceback.format_exc()
        print(f"\n  ✗ ROLLED BACK: All changes undone.")
        print(f"  Error: {e}")
        print(f"  Full traceback:\n{error_detail}")

        # Log failed run (this commits independently)
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

        raise  # Re-raise so main.py can send the alert

    finally:
        conn.close()
        print(f"\n{'='*60}")
        print(f"  PIPELINE END: {portal.upper()}")
        print(f"{'='*60}\n")
