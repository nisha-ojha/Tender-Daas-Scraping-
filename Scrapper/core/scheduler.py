import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import datetime, timezone, timedelta

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
except ImportError:
    print("[ERROR] APScheduler not installed. Run: pip install apscheduler pytz")
    sys.exit(1)

from core.pipeline import run_pipeline
from core.alerts import alert_info, alert_error, alert_success

PORTAL_SCHEDULES = {
    "seci": [(0,0),(4,0),(8,0),(12,0),(16,0),(20,0)],
    "cppp": [(1,0),(5,0),(9,0),(13,0),(17,0),(21,0)],
}

SKIP_IF_RAN_WITHIN_SECONDS = int(3.5 * 3600)

def run_portal_job(portal):
    if _ran_recently(portal):
        return
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = f"run_{timestamp}_{portal}"
    print(f"\n{'='*60}\n  SCHEDULED JOB: {portal.upper()}\n  Batch: {batch_id}\n{'='*60}\n")
    alert_info(portal, "Scheduled run starting", batch_id)
    try:
        result = run_pipeline(portal=portal, batch_id=batch_id, stages="all")
        alert_success(portal, f"Done. New: {result['new']}, Updated: {result.get('updated',0)}", batch_id)
        print(f"\n  ✓ {portal.upper()} complete.")
    except Exception as e:
        alert_error(portal, f"FAILED: {str(e)[:300]}", batch_id)
        print(f"\n  ✗ {portal.upper()} failed: {e}")

def _ran_recently(portal):
    try:
        from core.db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT finished_at FROM scraper_runs WHERE portal=%s AND status='success' ORDER BY finished_at DESC LIMIT 1", (portal,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0]:
            finished_at = row[0]
            if finished_at.tzinfo is None:
                finished_at = finished_at.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - finished_at).total_seconds()
            if age < SKIP_IF_RAN_WITHIN_SECONDS:
                print(f"  [{portal.upper()}] Ran {round(age/3600,1)}h ago — skipping")
                return True
        return False
    except Exception as e:
        print(f"  [{portal.upper()}] DB check failed: {e} — proceeding")
        return False

def create_scheduler(background=False):
    Scheduler = BackgroundScheduler if background else BlockingScheduler
    scheduler = Scheduler(timezone="Asia/Kolkata")
    for portal, times in PORTAL_SCHEDULES.items():
        for i, (h, m) in enumerate(times):
            scheduler.add_job(run_portal_job, trigger=CronTrigger(hour=h, minute=m, timezone="Asia/Kolkata"),
                args=[portal], id=f"{portal}_slot_{i}", misfire_grace_time=3600, replace_existing=True, max_instances=1)
    return scheduler

def main():
    parser = argparse.ArgumentParser(description="Tender DAAS Scheduler")
    parser.add_argument("--once", metavar="PORTAL")
    parser.add_argument("--no-skip", action="store_true")
    args = parser.parse_args()

    if getattr(args, "no_skip", False):
        global SKIP_IF_RAN_WITHIN_SECONDS
        SKIP_IF_RAN_WITHIN_SECONDS = 0

    if args.once:
        portals = list(PORTAL_SCHEDULES.keys()) if args.once == "all" else [args.once]
        for portal in portals:
            if portal not in PORTAL_SCHEDULES:
                print(f"[ERROR] Unknown portal '{portal}'. Known: {list(PORTAL_SCHEDULES)}")
                sys.exit(1)
            run_portal_job(portal)
        print("\n  Done.")
        return

    print("\n" + "="*60)
    print("  Tender DAAS — Scheduler")
    print("="*60)
    for portal, times in PORTAL_SCHEDULES.items():
        print(f"  {portal.upper():10} → {', '.join(f'{h:02d}:{m:02d}' for h,m in times)}")
    print("\nPress Ctrl+C to stop.\n")
    scheduler = create_scheduler(background=False)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\nScheduler stopped.")
        scheduler.shutdown(wait=False)

if __name__ == "__main__":
    main()
