"""
dashboard/app.py
----------------
Production-grade Admin Dashboard for Tender DAAS.

Run:
  cd "A:\\Tender DAAS\\Scrapper"
  uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload

Then open: http://localhost:8000
"""

import os
import sys
import json
import threading
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from core.db import get_connection

app = FastAPI(title="Tender DAAS Admin Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Active job tracking (in-memory) ──────────────────────────
_active_jobs: dict[str, dict] = {}   # job_id → {portal, status, started, logs}
_job_lock = threading.Lock()

AVAILABLE_PORTALS = ["seci", "cppp"]

# ══════════════════════════════════════════════════════════════
# HELPER
# ══════════════════════════════════════════════════════════════

def db_query(sql: str, params=None, fetchone=False):
    conn = get_connection()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        if fetchone:
            return cur.fetchone()
        return cur.fetchall()
    finally:
        conn.close()

def db_execute(sql: str, params=None):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        conn.commit()
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════
# 1. OVERVIEW STATS
# ══════════════════════════════════════════════════════════════

@app.get("/api/stats")
def get_stats():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM tenders")
        total = cur.fetchone()[0]

        cur.execute("SELECT status, COUNT(*) FROM tenders GROUP BY status ORDER BY count DESC")
        by_status = [{"status": r[0], "count": r[1]} for r in cur.fetchall()]

        cur.execute("SELECT category, COUNT(*) FROM tenders GROUP BY category ORDER BY count DESC LIMIT 10")
        by_category = [{"category": r[0], "count": r[1]} for r in cur.fetchall()]

        cur.execute("SELECT COUNT(*) FROM tenders WHERE emd_amount IS NOT NULL")
        with_emd = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM tenders WHERE document_count > 0")
        with_docs = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(document_count), 0) FROM tenders")
        total_docs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM raw_records")
        raw_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM raw_records WHERE raw_data::jsonb ? 'detail'")
        with_detail = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM scraper_runs WHERE status='success'")
        success_runs = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM scraper_runs WHERE status='error'")
        failed_runs = cur.fetchone()[0]

        # Tenders added in last 24h
        cur.execute("SELECT COUNT(*) FROM tenders WHERE created_at > NOW() - INTERVAL '24 hours'")
        added_24h = cur.fetchone()[0]

        # Tenders added in last 7 days
        cur.execute("SELECT COUNT(*) FROM tenders WHERE created_at > NOW() - INTERVAL '7 days'")
        added_7d = cur.fetchone()[0]

        return {
            "total_tenders": total,
            "by_status": by_status,
            "by_category": by_category,
            "with_emd": with_emd,
            "with_documents": with_docs,
            "total_document_urls": int(total_docs),
            "raw_records": raw_count,
            "with_detail_page": with_detail,
            "success_runs": success_runs,
            "failed_runs": failed_runs,
            "added_24h": added_24h,
            "added_7d": added_7d,
        }
    finally:
        cur.close()
        conn.close()

# ══════════════════════════════════════════════════════════════
# 2. RUN HISTORY
# ══════════════════════════════════════════════════════════════

@app.get("/api/runs")
def get_runs(
    portal: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
):
    conditions = []
    params = []
    if portal:
        conditions.append("portal = %s")
        params.append(portal)
    if status:
        conditions.append("status = %s")
        params.append(status)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = db_query(f"""
        SELECT
            id, portal, batch_id, status,
            records_found, records_new, records_updated,
            error_message, started_at, finished_at,
            EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))::int AS duration_sec
        FROM scraper_runs
        {where}
        ORDER BY started_at DESC
        LIMIT %s
    """, params)

    return {"runs": [dict(r) for r in rows]}


@app.get("/api/runs/{batch_id}")
def get_run_detail(batch_id: str):
    run = db_query(
        "SELECT * FROM scraper_runs WHERE batch_id = %s",
        (batch_id,), fetchone=True
    )
    if not run:
        raise HTTPException(404, "Run not found")

    # Raw records for this batch
    raw = db_query(
        "SELECT id, processed, error_message, scraped_at FROM raw_records WHERE batch_id = %s ORDER BY id",
        (batch_id,)
    )
    # Tenders from this batch
    tenders = db_query(
        "SELECT reference_number, title, category, status, emd_amount FROM tenders WHERE batch_id = %s ORDER BY created_at",
        (batch_id,)
    )

    return {
        "run": dict(run),
        "raw_records": [dict(r) for r in raw],
        "tenders": [dict(t) for t in tenders],
    }

# ══════════════════════════════════════════════════════════════
# 3. PORTAL HEALTH (last 30 days)
# ══════════════════════════════════════════════════════════════

@app.get("/api/health")
def get_health():
    result = {}
    for portal in AVAILABLE_PORTALS:
        # Last 30 days of runs
        rows = db_query("""
            SELECT
                DATE(started_at) AS run_date,
                status,
                records_new,
                finished_at,
                EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))::int AS duration_sec
            FROM scraper_runs
            WHERE portal = %s AND started_at > NOW() - INTERVAL '30 days'
            ORDER BY started_at DESC
        """, (portal,))

        runs_list = [dict(r) for r in rows]

        # Last successful run
        last_ok = db_query("""
            SELECT finished_at FROM scraper_runs
            WHERE portal = %s AND status = 'success'
            ORDER BY finished_at DESC LIMIT 1
        """, (portal,), fetchone=True)

        total = len(runs_list)
        success_count = sum(1 for r in runs_list if r["status"] == "success")
        uptime_pct = round(success_count / total * 100) if total > 0 else 0

        # Day grid: last 30 days
        day_grid = {}
        for r in runs_list:
            d = str(r["run_date"])
            if d not in day_grid:
                day_grid[d] = r["status"]
            elif r["status"] == "error":
                day_grid[d] = "error"

        result[portal] = {
            "uptime_pct": uptime_pct,
            "total_runs": total,
            "success_runs": success_count,
            "last_success": last_ok["finished_at"].isoformat() if last_ok and last_ok["finished_at"] else None,
            "day_grid": day_grid,
            "recent_runs": runs_list[:10],
        }

    return result

# ══════════════════════════════════════════════════════════════
# 4. RESPONSE TIMES
# ══════════════════════════════════════════════════════════════

@app.get("/api/response-times")
def get_response_times():
    rows = db_query("""
        SELECT
            portal, batch_id,
            EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))::int AS duration_sec,
            records_found, records_new, status,
            started_at
        FROM scraper_runs
        WHERE started_at > NOW() - INTERVAL '30 days'
        ORDER BY started_at ASC
    """)

    by_portal: dict[str, list] = {}
    for r in rows:
        p = r["portal"]
        if p not in by_portal:
            by_portal[p] = []
        by_portal[p].append({
            "batch_id": r["batch_id"],
            "duration_sec": r["duration_sec"],
            "records_found": r["records_found"],
            "records_new": r["records_new"],
            "status": r["status"],
            "started_at": r["started_at"].isoformat() if r["started_at"] else None,
        })

    # Average per portal
    averages = {}
    for p, runs in by_portal.items():
        durations = [r["duration_sec"] for r in runs if r["duration_sec"]]
        averages[p] = round(sum(durations) / len(durations)) if durations else 0

    return {"by_portal": by_portal, "averages": averages}

# ══════════════════════════════════════════════════════════════
# 5. DB RECORDS TRACKER
# ══════════════════════════════════════════════════════════════

@app.get("/api/db-stats")
def get_db_stats():
    # Total per portal
    totals = db_query("""
        SELECT source_portal, COUNT(*) AS total
        FROM tenders GROUP BY source_portal ORDER BY total DESC
    """)

    # Growth: records added per run (last 20 runs)
    growth = db_query("""
        SELECT sr.portal, sr.batch_id, sr.records_new, sr.started_at
        FROM scraper_runs sr
        WHERE sr.status = 'success'
        ORDER BY sr.started_at DESC
        LIMIT 20
    """)

    # Raw records stats
    raw_stats = db_query("""
        SELECT portal, COUNT(*) AS total,
               SUM(CASE WHEN processed THEN 1 ELSE 0 END) AS processed,
               SUM(CASE WHEN error_message IS NOT NULL AND error_message NOT LIKE 'Skipped%%'
                             AND error_message NOT LIKE 'Duplicate%%' THEN 1 ELSE 0 END) AS errors
        FROM raw_records GROUP BY portal
    """)

    return {
        "totals": [dict(r) for r in totals],
        "growth": [dict(r) for r in growth],
        "raw_stats": [dict(r) for r in raw_stats],
    }

# ══════════════════════════════════════════════════════════════
# 6. RUN DIFF VIEWER
# ══════════════════════════════════════════════════════════════

@app.get("/api/diff")
def get_diff(run1: str = Query(...), run2: str = Query(...)):
    def get_batch_tenders(batch_id):
        rows = db_query("""
            SELECT reference_number, title, status, emd_amount, deadline,
                   category, document_count, date_published
            FROM tenders WHERE batch_id = %s
        """, (batch_id,))
        return {r["reference_number"]: dict(r) for r in rows if r["reference_number"]}

    b1 = get_batch_tenders(run1)
    b2 = get_batch_tenders(run2)

    added = [v for k, v in b2.items() if k not in b1]
    removed = [v for k, v in b1.items() if k not in b2]

    # Changed: same ref, different field values
    changed = []
    watch_fields = ["status", "emd_amount", "deadline", "document_count"]
    for ref in set(b1) & set(b2):
        diffs = {}
        for f in watch_fields:
            v1 = str(b1[ref].get(f, ""))
            v2 = str(b2[ref].get(f, ""))
            if v1 != v2:
                diffs[f] = {"before": v1, "after": v2}
        if diffs:
            changed.append({"reference_number": ref, "title": b1[ref]["title"], "changes": diffs})

    return {
        "run1": run1, "run2": run2,
        "added": added, "removed": removed, "changed": changed,
        "summary": {
            "added_count": len(added),
            "removed_count": len(removed),
            "changed_count": len(changed),
        }
    }

# ══════════════════════════════════════════════════════════════
# 7. JOBS MONITOR
# ══════════════════════════════════════════════════════════════

@app.get("/api/jobs")
def get_jobs():
    with _job_lock:
        jobs = list(_active_jobs.values())

    # Also get recent DB runs that are 'running' (in case of crashed workers)
    running_db = db_query("""
        SELECT portal, batch_id, started_at
        FROM scraper_runs
        WHERE status = 'running'
        ORDER BY started_at DESC LIMIT 5
    """)

    return {
        "active_jobs": jobs,
        "running_in_db": [dict(r) for r in running_db],
    }


def _run_pipeline_job(job_id: str, portal: str, batch_id: str):
    """Background thread that runs the pipeline."""
    from core.pipeline import run_pipeline
    from core.alerts import alert_error, alert_success

    with _job_lock:
        _active_jobs[job_id]["status"] = "running"
        _active_jobs[job_id]["logs"].append(f"[{_now()}] Pipeline starting for {portal}")

    try:
        result = run_pipeline(portal=portal, batch_id=batch_id, stages="all")
        with _job_lock:
            _active_jobs[job_id]["status"] = "success"
            _active_jobs[job_id]["result"] = result
            _active_jobs[job_id]["logs"].append(
                f"[{_now()}] Done — New: {result['new']}, Raw: {result['raw_records']}"
            )
    except Exception as e:
        with _job_lock:
            _active_jobs[job_id]["status"] = "error"
            _active_jobs[job_id]["error"] = str(e)[:500]
            _active_jobs[job_id]["logs"].append(f"[{_now()}] FAILED: {e}")


def _now():
    return datetime.now().strftime("%H:%M:%S")


@app.post("/api/trigger/{portal}")
def trigger_portal(portal: str, background_tasks: BackgroundTasks):
    if portal not in AVAILABLE_PORTALS:
        raise HTTPException(400, f"Unknown portal. Choose from: {AVAILABLE_PORTALS}")

    # Check if already running
    with _job_lock:
        for job in _active_jobs.values():
            if job["portal"] == portal and job["status"] == "running":
                raise HTTPException(409, f"{portal} is already running (job {job['job_id']})")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = f"run_{ts}_{portal}_manual"
    job_id = f"job_{ts}_{portal}"

    with _job_lock:
        _active_jobs[job_id] = {
            "job_id": job_id,
            "portal": portal,
            "batch_id": batch_id,
            "status": "pending",
            "started": datetime.now().isoformat(),
            "logs": [f"[{_now()}] Job queued for {portal}"],
            "result": None,
            "error": None,
        }

    # Run in background thread
    t = threading.Thread(
        target=_run_pipeline_job,
        args=(job_id, portal, batch_id),
        daemon=True,
    )
    t.start()

    return {"job_id": job_id, "batch_id": batch_id, "status": "started"}


@app.post("/api/kill/{job_id}")
def kill_job(job_id: str):
    with _job_lock:
        if job_id not in _active_jobs:
            raise HTTPException(404, "Job not found")
        _active_jobs[job_id]["status"] = "killed"
        _active_jobs[job_id]["logs"].append(f"[{_now()}] Job killed by user")

    return {"job_id": job_id, "status": "killed"}


@app.get("/api/job-logs/{job_id}")
def get_job_logs(job_id: str):
    with _job_lock:
        if job_id not in _active_jobs:
            raise HTTPException(404, "Job not found")
        return _active_jobs[job_id]

# ══════════════════════════════════════════════════════════════
# 8. ALERTS
# ══════════════════════════════════════════════════════════════

@app.get("/api/alerts")
def get_alerts():
    alerts = []

    # Failed runs
    failed = db_query("""
        SELECT portal, batch_id, error_message, started_at
        FROM scraper_runs
        WHERE status = 'error' AND started_at > NOW() - INTERVAL '7 days'
        ORDER BY started_at DESC LIMIT 10
    """)
    for r in failed:
        alerts.append({
            "severity": "critical",
            "type": "scraper_failure",
            "portal": r["portal"],
            "message": r["error_message"][:120] if r["error_message"] else "Unknown error",
            "time": r["started_at"].isoformat() if r["started_at"] else None,
            "batch_id": r["batch_id"],
        })

    # Zero records runs
    zero = db_query("""
        SELECT portal, batch_id, started_at
        FROM scraper_runs
        WHERE status = 'success' AND records_new = 0
          AND started_at > NOW() - INTERVAL '7 days'
        ORDER BY started_at DESC LIMIT 5
    """)
    for r in zero:
        alerts.append({
            "severity": "warning",
            "type": "zero_records",
            "portal": r["portal"],
            "message": "Successful run but zero new records extracted",
            "time": r["started_at"].isoformat() if r["started_at"] else None,
            "batch_id": r["batch_id"],
        })

    # Stale portals (no run in >25 hours)
    for portal in AVAILABLE_PORTALS:
        last = db_query("""
            SELECT finished_at FROM scraper_runs
            WHERE portal = %s AND status = 'success'
            ORDER BY finished_at DESC LIMIT 1
        """, (portal,), fetchone=True)

        if not last or not last["finished_at"]:
            alerts.append({
                "severity": "warning",
                "type": "never_run",
                "portal": portal,
                "message": f"{portal.upper()} has never had a successful run",
                "time": None,
            })
        else:
            age_hours = (datetime.now(timezone.utc) - last["finished_at"].replace(tzinfo=timezone.utc)).total_seconds() / 3600
            if age_hours > 25:
                alerts.append({
                    "severity": "warning",
                    "type": "stale_data",
                    "portal": portal,
                    "message": f"Last successful scrape was {round(age_hours)}h ago (threshold: 25h)",
                    "time": last["finished_at"].isoformat(),
                })

    alerts.sort(key=lambda a: (a["severity"] == "critical", a["time"] or ""), reverse=True)
    return {"alerts": alerts, "count": len(alerts)}

# ══════════════════════════════════════════════════════════════
# 9. ERROR INTELLIGENCE
# ══════════════════════════════════════════════════════════════

@app.get("/api/errors")
def get_errors():
    # Categorise errors from raw_records
    raw_errors = db_query("""
        SELECT portal, error_message, scraped_at
        FROM raw_records
        WHERE error_message IS NOT NULL
          AND error_message NOT LIKE 'Skipped%%'
          AND error_message NOT LIKE 'Duplicate%%'
        ORDER BY scraped_at DESC
        LIMIT 200
    """)

    categories = {
        "Timeout": 0, "Structure Change": 0,
        "DB Error": 0, "Parse Error": 0, "Other": 0,
    }
    detailed = []

    for r in raw_errors:
        msg = (r["error_message"] or "").lower()
        if "timeout" in msg or "time out" in msg:
            cat = "Timeout"
        elif "column" in msg or "key" in msg or "attribute" in msg or "selector" in msg:
            cat = "Structure Change"
        elif "psycopg" in msg or "db error" in msg or "relation" in msg:
            cat = "DB Error"
        elif "parse" in msg or "date" in msg or "value" in msg or "int" in msg:
            cat = "Parse Error"
        else:
            cat = "Other"

        categories[cat] += 1
        detailed.append({
            "portal": r["portal"],
            "category": cat,
            "message": r["error_message"][:100] if r["error_message"] else "",
            "time": r["scraped_at"].isoformat() if r["scraped_at"] else None,
        })

    # Run-level errors
    run_errors = db_query("""
        SELECT portal, error_message, started_at
        FROM scraper_runs WHERE status = 'error'
        ORDER BY started_at DESC LIMIT 10
    """)

    return {
        "categories": [{"name": k, "count": v} for k, v in categories.items() if v > 0],
        "detailed": detailed[:50],
        "run_errors": [dict(r) for r in run_errors],
    }

# ══════════════════════════════════════════════════════════════
# 10. TENDER ANALYTICS
# ══════════════════════════════════════════════════════════════

@app.get("/api/analytics")
def get_analytics():
    # By status
    by_status = db_query("SELECT status, COUNT(*) AS cnt FROM tenders GROUP BY status")

    # By category
    by_category = db_query("""
        SELECT category, COUNT(*) AS cnt FROM tenders
        GROUP BY category ORDER BY cnt DESC LIMIT 15
    """)

    # By source portal
    by_portal = db_query("SELECT source_portal, COUNT(*) AS cnt FROM tenders GROUP BY source_portal")

    # New tenders per day (last 30 days)
    daily = db_query("""
        SELECT DATE(created_at) AS day, COUNT(*) AS cnt
        FROM tenders
        WHERE created_at > NOW() - INTERVAL '30 days'
        GROUP BY day ORDER BY day ASC
    """)

    # By state
    by_state = db_query("""
        SELECT state, COUNT(*) AS cnt FROM tenders
        WHERE state IS NOT NULL
        GROUP BY state ORDER BY cnt DESC LIMIT 15
    """)

    # Deadline distribution: expiring soon
    expiring = db_query("""
        SELECT
            SUM(CASE WHEN deadline < NOW() + INTERVAL '3 days' AND deadline > NOW() THEN 1 ELSE 0 END) AS in_3_days,
            SUM(CASE WHEN deadline < NOW() + INTERVAL '7 days' AND deadline > NOW() THEN 1 ELSE 0 END) AS in_7_days,
            SUM(CASE WHEN deadline < NOW() + INTERVAL '30 days' AND deadline > NOW() THEN 1 ELSE 0 END) AS in_30_days,
            SUM(CASE WHEN deadline < NOW() THEN 1 ELSE 0 END) AS expired
        FROM tenders WHERE status = 'open'
    """, fetchone=True)

    return {
        "by_status": [dict(r) for r in by_status],
        "by_category": [dict(r) for r in by_category],
        "by_portal": [dict(r) for r in by_portal],
        "daily_new": [dict(r) for r in daily],
        "by_state": [dict(r) for r in by_state],
        "expiring": dict(expiring) if expiring else {},
    }

# ══════════════════════════════════════════════════════════════
# 11. DATA FRESHNESS
# ══════════════════════════════════════════════════════════════

@app.get("/api/freshness")
def get_freshness():
    result = {}
    for portal in AVAILABLE_PORTALS:
        last_run = db_query("""
            SELECT finished_at, records_new, records_found, started_at
            FROM scraper_runs
            WHERE portal = %s AND status = 'success'
            ORDER BY finished_at DESC LIMIT 1
        """, (portal,), fetchone=True)

        if last_run and last_run["finished_at"]:
            age_hours = (
                datetime.now(timezone.utc)
                - last_run["finished_at"].replace(tzinfo=timezone.utc)
            ).total_seconds() / 3600

            result[portal] = {
                "last_scrape": last_run["finished_at"].isoformat(),
                "age_hours": round(age_hours, 1),
                "is_stale": age_hours > 25,
                "records_new": last_run["records_new"],
                "records_found": last_run["records_found"],
            }
        else:
            result[portal] = {
                "last_scrape": None,
                "age_hours": None,
                "is_stale": True,
                "records_new": 0,
                "records_found": 0,
            }

    return result

# ══════════════════════════════════════════════════════════════
# 12. PERFORMANCE METRICS
# ══════════════════════════════════════════════════════════════

@app.get("/api/performance")
def get_performance():
    rows = db_query("""
        SELECT
            portal, batch_id, status,
            records_found, records_new,
            EXTRACT(EPOCH FROM (COALESCE(finished_at, NOW()) - started_at))::int AS duration_sec,
            started_at
        FROM scraper_runs
        WHERE started_at > NOW() - INTERVAL '30 days'
        ORDER BY started_at DESC
    """)

    metrics = []
    for r in rows:
        dur = r["duration_sec"] or 1
        found = r["records_found"] or 0
        efficiency = round(found / (dur / 60), 2) if dur > 0 else 0  # records per minute

        metrics.append({
            "portal": r["portal"],
            "batch_id": r["batch_id"],
            "status": r["status"],
            "records_found": found,
            "records_new": r["records_new"] or 0,
            "duration_sec": dur,
            "efficiency": efficiency,
            "started_at": r["started_at"].isoformat() if r["started_at"] else None,
        })

    # Aggregate per portal
    portal_agg = {}
    for m in metrics:
        p = m["portal"]
        if p not in portal_agg:
            portal_agg[p] = {"runs": 0, "total_records": 0, "total_sec": 0}
        portal_agg[p]["runs"] += 1
        portal_agg[p]["total_records"] += m["records_found"]
        portal_agg[p]["total_sec"] += m["duration_sec"]

    aggregates = {
        p: {
            "avg_duration": round(v["total_sec"] / v["runs"]) if v["runs"] else 0,
            "avg_records": round(v["total_records"] / v["runs"]) if v["runs"] else 0,
            "total_runs": v["runs"],
        }
        for p, v in portal_agg.items()
    }

    return {"metrics": metrics, "aggregates": aggregates}

# ══════════════════════════════════════════════════════════════
# 13. DATA QUALITY
# ══════════════════════════════════════════════════════════════

@app.get("/api/quality")
def get_quality():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM tenders")
        total = cur.fetchone()[0]
        if total == 0:
            return {"checks": {}, "total": 0, "score": 0}

        fields = [
            ("reference_number", "Reference Number"),
            ("title", "Title"),
            ("date_published", "Publication Date"),
            ("deadline", "Deadline"),
            ("emd_amount", "EMD Amount"),
            ("category", "Category"),
            ("bid_opening_date", "Bid Opening Date"),
            ("state", "State"),
        ]

        checks = {}
        score_sum = 0

        for col, label in fields:
            cur.execute(f"SELECT COUNT(*) FROM tenders WHERE {col} IS NULL")
            missing = cur.fetchone()[0]
            pct = round((total - missing) / total * 100)
            score_sum += pct
            checks[label] = {"filled": total - missing, "missing": missing, "pct": pct}

        cur.execute("SELECT COUNT(*) FROM tenders WHERE document_count > 0")
        with_docs = cur.fetchone()[0]
        doc_pct = round(with_docs / total * 100)
        score_sum += doc_pct
        checks["Has Documents"] = {"filled": with_docs, "missing": total - with_docs, "pct": doc_pct}

        cur.execute("SELECT COUNT(*) FROM tenders WHERE category != 'Uncategorized'")
        cat_filled = cur.fetchone()[0]
        cat_pct = round(cat_filled / total * 100)
        score_sum += cat_pct
        checks["Categorized"] = {"filled": cat_filled, "missing": total - cat_filled, "pct": cat_pct}

        overall_score = round(score_sum / len(checks))

        return {"checks": checks, "total": total, "score": overall_score}
    finally:
        cur.close()
        conn.close()

# ══════════════════════════════════════════════════════════════
# 14. DATA VALIDATION QUERIES
# ══════════════════════════════════════════════════════════════

VALIDATION_CHECKS = [
    {
        "id": "dup_ref",
        "name": "Duplicate Reference Numbers",
        "severity": "critical",
        "sql": """
            SELECT reference_number, organization_short, COUNT(*) AS count
            FROM tenders
            WHERE reference_number IS NOT NULL
            GROUP BY reference_number, organization_short
            HAVING COUNT(*) > 1
            ORDER BY count DESC LIMIT 20
        """,
        "expect_empty": True,
        "description": "Same reference_number + org should never appear twice",
    },
    {
        "id": "null_title",
        "name": "Tenders with NULL Title",
        "severity": "critical",
        "sql": "SELECT id, reference_number, source_portal FROM tenders WHERE title IS NULL LIMIT 20",
        "expect_empty": True,
        "description": "Every tender must have a title",
    },
    {
        "id": "past_deadline_open",
        "name": "Open Tenders with Expired Deadline",
        "severity": "warning",
        "sql": """
            SELECT reference_number, title, deadline, status
            FROM tenders
            WHERE status = 'open' AND deadline < NOW()
            ORDER BY deadline DESC LIMIT 20
        """,
        "expect_empty": False,
        "description": "These tenders are marked open but their deadline has passed — may need status update",
    },
    {
        "id": "no_docs",
        "name": "Tenders with Zero Documents",
        "severity": "warning",
        "sql": """
            SELECT reference_number, title, source_portal
            FROM tenders WHERE document_count = 0 OR document_count IS NULL
            ORDER BY created_at DESC LIMIT 20
        """,
        "expect_empty": False,
        "description": "Most tenders should have at least one document",
    },
    {
        "id": "future_published",
        "name": "Tenders Published in Future",
        "severity": "critical",
        "sql": """
            SELECT reference_number, title, date_published
            FROM tenders WHERE date_published > CURRENT_DATE LIMIT 20
        """,
        "expect_empty": True,
        "description": "Publication date should not be in the future — likely a parsing error",
    },
    {
        "id": "deadline_before_published",
        "name": "Deadline Before Publication Date",
        "severity": "critical",
        "sql": """
            SELECT reference_number, title, date_published, deadline::date
            FROM tenders
            WHERE deadline::date < date_published
              AND deadline IS NOT NULL AND date_published IS NOT NULL
            LIMIT 20
        """,
        "expect_empty": True,
        "description": "Deadline cannot be before publication date — parsing error",
    },
    {
        "id": "unprocessed_raw",
        "name": "Unprocessed Raw Records",
        "severity": "warning",
        "sql": """
            SELECT portal, COUNT(*) AS count
            FROM raw_records WHERE processed = FALSE
            GROUP BY portal
        """,
        "expect_empty": True,
        "description": "All raw records should be processed after each run",
    },
    {
        "id": "uncategorized",
        "name": "Uncategorized Tenders",
        "severity": "info",
        "sql": """
            SELECT source_portal, COUNT(*) AS count
            FROM tenders WHERE category = 'Uncategorized' OR category IS NULL
            GROUP BY source_portal
        """,
        "expect_empty": False,
        "description": "High uncategorized count means classify_tender() needs more keywords",
    },
    {
        "id": "emd_zero",
        "name": "Zero EMD Amount (non-null)",
        "severity": "info",
        "sql": """
            SELECT reference_number, title
            FROM tenders WHERE emd_amount = 0
            LIMIT 20
        """,
        "expect_empty": False,
        "description": "EMD = 0 may be correct (nil EMD) or a parsing failure",
    },
    {
        "id": "raw_errors",
        "name": "Raw Records with Errors",
        "severity": "warning",
        "sql": """
            SELECT portal, error_message, COUNT(*) AS count
            FROM raw_records
            WHERE error_message IS NOT NULL
              AND error_message NOT LIKE 'Skipped%%'
              AND error_message NOT LIKE 'Duplicate%%'
            GROUP BY portal, error_message
            ORDER BY count DESC LIMIT 20
        """,
        "expect_empty": True,
        "description": "Errors during normalization — check date/value parsing",
    },
]

@app.get("/api/validate")
def run_validation():
    results = []
    conn = get_connection()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        for check in VALIDATION_CHECKS:
            try:
                cur.execute(check["sql"])
                rows = cur.fetchall()
                data = [dict(r) for r in rows]
                has_issues = len(data) > 0

                if check["expect_empty"]:
                    passed = not has_issues
                else:
                    passed = True  # informational only

                results.append({
                    "id": check["id"],
                    "name": check["name"],
                    "severity": check["severity"],
                    "description": check["description"],
                    "passed": passed,
                    "row_count": len(data),
                    "data": data[:10],  # Show first 10 rows
                    "expect_empty": check["expect_empty"],
                })
            except Exception as e:
                results.append({
                    "id": check["id"],
                    "name": check["name"],
                    "severity": "critical",
                    "description": check["description"],
                    "passed": False,
                    "row_count": 0,
                    "data": [],
                    "error": str(e),
                    "expect_empty": check["expect_empty"],
                })

        passed = sum(1 for r in results if r["passed"])
        return {
            "results": results,
            "summary": {
                "total": len(results),
                "passed": passed,
                "failed": len(results) - passed,
                "score": round(passed / len(results) * 100) if results else 0,
            }
        }
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════
# 15. RECENT TENDERS
# ══════════════════════════════════════════════════════════════

@app.get("/api/recent-tenders")
def get_recent_tenders(limit: int = 50, portal: Optional[str] = None):
    where = "WHERE t.source_portal = %s" if portal else ""
    params = [portal, limit] if portal else [limit]

    rows = db_query(f"""
        SELECT reference_number, title, status, category, state,
               emd_amount, value_display, date_published, deadline,
               document_count, source_portal, created_at
        FROM tenders t
        {where}
        ORDER BY created_at DESC
        LIMIT %s
    """, params)

    return {"tenders": [dict(r) for r in rows]}

# ══════════════════════════════════════════════════════════════
# SERVE FRONTEND
# ══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def index():
    # Read from file if it exists, otherwise serve inline
    frontend_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(frontend_path):
        with open(frontend_path, encoding="utf-8") as f:
            return f.read()
    return HTMLResponse("<h1>Frontend not found. Place index.html in dashboard/</h1>", status_code=500)
