"""
main.py
-------
THE ENTRY POINT. This is what you run.

USAGE:
  python main.py --portal seci              Run SECI scraper (all stages)
  python main.py --portal cppp              Run CPPP scraper (all stages)
  python main.py --portal all               Run ALL portals one by one
  python main.py --portal seci --stage scrape   Run ONLY the scrape stage

This file:
  1. Reads command-line arguments
  2. Generates a unique batch_id for tracking
  3. Calls pipeline.py to run all stages
  4. Sends success/error alert via webhook
"""

import argparse
import sys
from datetime import datetime

from core.pipeline import run_pipeline
from core.alerts import alert_success, alert_error, alert_info


# ── List of all available portals ──
# Add new portals here as you build them
AVAILABLE_PORTALS = ["seci", "cppp"]  # TODO: Add "cppp" when ready


def main():
    # ── Parse command-line arguments ──
    parser = argparse.ArgumentParser(
        description="Tender DAAS — Scraping Pipeline",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--portal",
        required=True,
        choices=AVAILABLE_PORTALS + ["all"],
        help="Which portal to scrape:\n"
             "  seci  — Solar Energy Corporation of India\n"
             "  all   — Run all portals sequentially",
    )
    parser.add_argument(
        "--stage",
        default="all",
        choices=["all", "scrape", "normalize", "pdf", "dedup"],

        help="Which stage to run (default: all)",
    )

    args = parser.parse_args()

    # ── Determine which portals to run ──
    if args.portal == "all":
        portals = AVAILABLE_PORTALS
    else:
        portals = [args.portal]

    # ── Generate batch ID ──
    # Format: run_YYYYMMDD_HHMMSS_portal
    # Example: run_20260322_143000_seci
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "=" * 60)
    print("  TENDER DAAS — PIPELINE RUNNER")
    print(f"  Portals: {', '.join(portals)}")
    print(f"  Stages: {args.stage}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # ── Run each portal ──
    total_success = 0
    total_failed = 0

    for portal in portals:
        batch_id = f"run_{timestamp}_{portal}"

        alert_info(portal, f"Pipeline starting (stages: {args.stage})", batch_id)

        try:
            result = run_pipeline(
                portal=portal,
                batch_id=batch_id,
                stages=args.stage,
            )

            alert_success(
                portal,
                f"Pipeline finished. New: {result['new']}, "
                f"Updated: {result['updated']}, "
                f"Raw records: {result['raw_records']}",
                batch_id,
            )
            total_success += 1

        except Exception as e:
            alert_error(
                portal,
                f"Pipeline FAILED: {str(e)[:300]}",
                batch_id,
            )
            total_failed += 1
            # Continue to next portal — don't stop everything
            continue

    # ── Summary ──
    print("\n" + "=" * 60)
    print(f"  ALL DONE: {total_success} succeeded, {total_failed} failed")
    print("=" * 60 + "\n")

    # Exit with error code if any portal failed
    if total_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
