"""
core/scheduler.py
-----------------
Automated scheduler for the Tender DAAS pipeline.

USAGE:
  # Start the scheduler (runs forever, Ctrl+C to stop):
  python core/scheduler.py

  # Run a single portal right now and exit (useful for testing):
  python core/scheduler.py --once seci
  python core/scheduler.py --once cppp

  # Run all portals once and exit:
  python core/scheduler.py --once all

SCHEDULES (IST):
  SECI  → 06:00 daily
  CPPP  → 07:30 daily  (offset to avoid DB contention)

WINDOWS TASK SCHEDULER ALTERNATIVE:
  If you prefer Windows Task Scheduler instead of running this process:
  1. Open Task Scheduler → Create Basic Task
  2. Set trigger: Daily at 06:00
  3. Action: Start a program
     Program: A:\\Tender DAAS\\Scrapper\\venv\\Scripts\\python.exe
     Arguments: core/scheduler.py --once seci
     Start in: A:\\Tender DAAS\\Scrapper

REQUIRES:
  pip install apscheduler pytz
"""

import argparse
import sys
from datetime import datetime

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
except ImportError:
    print("[ERROR] APScheduler not installed.")
    print("  Run: pip install apscheduler pytz")
    sys.exit(1)

from core.pipeline import run_pipeline
from core.alerts import alert_info, alert_error, alert_success


# ── Schedule configuration ────────────────────────────────────────────
# Add new portals here as you build them.
# Format: "portal_name": (hour, minute)  — IST timezone
PORTAL_SCHEDULES = {
    "seci": (6, 0),     # 06:00 IST
    "cppp": (7, 30),    # 07:30 IST
}


# ── Job function ─────────────────────────────────────────────────────

def run_portal_job(portal: str):
    """
    Runs the full pipeline for one portal.
    Called by the scheduler on its configured schedule.

    CRITICAL: This function NEVER raises an exception.
    If it raised, APScheduler would stop retrying that job.
    All errors are caught, logged, and alerted.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = f"run_{timestamp}_{portal}"

    print(f"\n{'='*60}")
    print(f"  SCHEDULED JOB: {portal.upper()}")
    print(f"  Time:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Batch: {batch_id}")
    print(f"{'='*60}\n")

    alert_info(portal, "Scheduled run starting", batch_id)

    try:
        result = run_pipeline(portal=portal, batch_id=batch_id, stages="all")
        alert_success(
            portal,
            f"Scheduled run complete — "
            f"New: {result['new']}, "
            f"Raw records: {result['raw_records']}",
            batch_id,
        )
        print(f"\n  ✓ Scheduled job {portal.upper()} complete.")

    except Exception as e:
        alert_error(
            portal,
            f"Scheduled run FAILED: {str(e)[:300]}",
            batch_id,
        )
        print(f"\n  ✗ Scheduled job {portal.upper()} failed: {e}")
        # Do NOT re-raise — the scheduler must keep running for the next job


# ── Scheduler factory ─────────────────────────────────────────────────

def create_scheduler(background: bool = False):
    """
    Create and configure an APScheduler instance.

    background=False → BlockingScheduler (blocks main thread — use in scheduler.py)
    background=True  → BackgroundScheduler (use inside a FastAPI/Flask server)
    """
    Scheduler = BackgroundScheduler if background else BlockingScheduler

    scheduler = Scheduler(timezone="Asia/Kolkata")

    for portal, (hour, minute) in PORTAL_SCHEDULES.items():
        scheduler.add_job(
            run_portal_job,
            trigger=CronTrigger(hour=hour, minute=minute, timezone="Asia/Kolkata"),
            args=[portal],
            id=f"{portal}_daily",
            name=f"{portal.upper()} Daily Scrape",
            misfire_grace_time=3600,  # Run up to 1hr late (handles sleep/restart)
            replace_existing=True,
            max_instances=1,          # Never run same portal twice simultaneously
        )

    return scheduler


def _log_job_event(event):
    """APScheduler event listener — logs job completion and errors."""
    if event.exception:
        print(f"  [SCHEDULER] Job {event.job_id} CRASHED: {event.exception}")
    else:
        print(f"  [SCHEDULER] Job {event.job_id} completed OK")


# ── Entry point ───────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Tender DAAS Scheduler",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--once",
        metavar="PORTAL",
        help=(
            "Run a portal immediately and exit (bypasses scheduler).\n"
            "  python core/scheduler.py --once seci\n"
            "  python core/scheduler.py --once all"
        ),
    )
    args = parser.parse_args()

    # ── --once mode: run immediately and exit ──
    if args.once:
        portals = (
            list(PORTAL_SCHEDULES.keys())
            if args.once == "all"
            else [args.once]
        )
        for portal in portals:
            if portal not in PORTAL_SCHEDULES:
                print(f"[ERROR] Unknown portal '{portal}'. Known: {list(PORTAL_SCHEDULES)}")
                sys.exit(1)
            run_portal_job(portal)
        print("\nDone.")
        return

    # ── Daemon mode: run on schedule forever ──
    print("\n" + "="*50)
    print("  Tender DAAS — Scheduler Starting")
    print("="*50)
    print("\nConfigured schedules (IST):")
    for portal, (h, m) in PORTAL_SCHEDULES.items():
        print(f"  {portal.upper():10} → {h:02d}:{m:02d} daily")
    print("\nPress Ctrl+C to stop.\n")

    scheduler = create_scheduler(background=False)
    scheduler.add_listener(_log_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
