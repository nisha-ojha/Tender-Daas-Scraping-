"""
dashboard/app.py
----------------
Monitoring dashboard for the Tender DAAS scraping pipeline.

Shows:
  - Scraper run history (success/fail, duration, record counts)
  - Tender statistics (by status, category, portal)
  - Recent tenders added
  - Error log
  - Document coverage
  - Data quality metrics

RUN:
  cd "A:\Tender DAAS\Scrapper"
  .\venv\Scripts\Activate.ps1
  uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload

Then open: http://localhost:8000
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.db import get_connection

app = FastAPI(title="Tender DAAS Dashboard")


# ══════════════════════════════════════════════════════════
# API ENDPOINTS
# ══════════════════════════════════════════════════════════

@app.get("/api/stats")
def get_stats():
    """Overall statistics."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Total tenders
        cur.execute("SELECT COUNT(*) FROM tenders")
        total = cur.fetchone()[0]

        # By status
        cur.execute("SELECT status, COUNT(*) FROM tenders GROUP BY status ORDER BY count DESC")
        by_status = [{"status": r[0], "count": r[1]} for r in cur.fetchall()]

        # By category
        cur.execute("SELECT category, COUNT(*) FROM tenders GROUP BY category ORDER BY count DESC")
        by_category = [{"category": r[0], "count": r[1]} for r in cur.fetchall()]

        # With EMD
        cur.execute("SELECT COUNT(*) FROM tenders WHERE emd_amount IS NOT NULL")
        with_emd = cur.fetchone()[0]

        # With documents
        cur.execute("SELECT COUNT(*) FROM tenders WHERE document_count > 0")
        with_docs = cur.fetchone()[0]

        # Total documents
        cur.execute("SELECT COALESCE(SUM(document_count), 0) FROM tenders")
        total_docs = cur.fetchone()[0]

        # Raw records
        cur.execute("SELECT COUNT(*) FROM raw_records")
        raw_count = cur.fetchone()[0]

        # With detail page data
        cur.execute("SELECT COUNT(*) FROM raw_records WHERE raw_data ? 'detail'")
        with_detail = cur.fetchone()[0]

        return {
            "total_tenders": total,
            "by_status": by_status,
            "by_category": by_category,
            "with_emd": with_emd,
            "with_documents": with_docs,
            "total_document_urls": total_docs,
            "raw_records": raw_count,
            "with_detail_page": with_detail,
        }
    finally:
        cur.close()
        conn.close()


@app.get("/api/runs")
def get_runs():
    """Recent scraper runs."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT portal, batch_id, status, records_found, records_new,
                   records_updated, error_message,
                   started_at, finished_at,
                   EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_sec
            FROM scraper_runs
            ORDER BY started_at DESC
            LIMIT 20
        """)
        runs = []
        for r in cur.fetchall():
            runs.append({
                "portal": r[0],
                "batch_id": r[1],
                "status": r[2],
                "records_found": r[3],
                "records_new": r[4],
                "records_updated": r[5],
                "error": r[6],
                "started_at": r[7].isoformat() if r[7] else None,
                "finished_at": r[8].isoformat() if r[8] else None,
                "duration_sec": round(r[9]) if r[9] else None,
            })
        return {"runs": runs}
    finally:
        cur.close()
        conn.close()


@app.get("/api/recent-tenders")
def get_recent_tenders():
    """Most recently added tenders."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT reference_number, title, status, category, state,
                   emd_amount, value_display, date_published, deadline,
                   document_count, created_at
            FROM tenders
            ORDER BY created_at DESC
            LIMIT 30
        """)
        tenders = []
        for r in cur.fetchall():
            tenders.append({
                "ref": r[0],
                "title": r[1],
                "status": r[2],
                "category": r[3],
                "state": r[4],
                "emd": r[5],
                "emd_display": r[6],
                "published": r[7].isoformat() if r[7] else None,
                "deadline": r[8].isoformat() if r[8] else None,
                "docs": r[9],
                "added": r[10].isoformat() if r[10] else None,
            })
        return {"tenders": tenders}
    finally:
        cur.close()
        conn.close()


@app.get("/api/errors")
def get_errors():
    """Recent errors from scraper runs and raw records."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Scraper run errors
        cur.execute("""
            SELECT portal, batch_id, error_message, started_at
            FROM scraper_runs
            WHERE status = 'error' AND error_message IS NOT NULL
            ORDER BY started_at DESC
            LIMIT 10
        """)
        run_errors = [
            {"portal": r[0], "batch": r[1], "error": r[2],
             "time": r[3].isoformat() if r[3] else None}
            for r in cur.fetchall()
        ]

        # Normalization errors
        cur.execute("""
            SELECT portal, error_message, scraped_at
            FROM raw_records
            WHERE error_message IS NOT NULL
              AND error_message NOT LIKE 'Skipped%%'
              AND error_message NOT LIKE 'Duplicate%%'
            ORDER BY scraped_at DESC
            LIMIT 10
        """)
        norm_errors = [
            {"portal": r[0], "error": r[1],
             "time": r[2].isoformat() if r[2] else None}
            for r in cur.fetchall()
        ]

        return {"run_errors": run_errors, "normalization_errors": norm_errors}
    finally:
        cur.close()
        conn.close()


@app.get("/api/quality")
def get_quality():
    """Data quality metrics."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        checks = {}

        cur.execute("SELECT COUNT(*) FROM tenders")
        total = cur.fetchone()[0]
        checks["total"] = total

        if total == 0:
            return {"checks": checks, "total": 0}

        # Missing fields
        fields = [
            ("reference_number", "Reference Number"),
            ("title", "Title"),
            ("date_published", "Publication Date"),
            ("deadline", "Deadline"),
            ("emd_amount", "EMD Amount"),
            ("category", "Category"),
            ("bid_opening_date", "Bid Opening Date"),
        ]

        for col, label in fields:
            cur.execute(f"SELECT COUNT(*) FROM tenders WHERE {col} IS NULL")
            missing = cur.fetchone()[0]
            pct = round((total - missing) / total * 100)
            checks[label] = {"filled": total - missing, "missing": missing, "pct": pct}

        # Documents
        cur.execute("SELECT COUNT(*) FROM tenders WHERE document_count > 0")
        with_docs = cur.fetchone()[0]
        checks["Has Documents"] = {
            "filled": with_docs, "missing": total - with_docs,
            "pct": round(with_docs / total * 100)
        }

        # Status = 'Uncategorized'
        cur.execute("SELECT COUNT(*) FROM tenders WHERE category = 'Uncategorized'")
        uncat = cur.fetchone()[0]
        checks["Categorized"] = {
            "filled": total - uncat, "missing": uncat,
            "pct": round((total - uncat) / total * 100)
        }

        return {"checks": checks, "total": total}
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════
# HTML DASHBOARD
# ══════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tender DAAS — Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a; color: #e2e8f0;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1e293b, #0f172a);
            border-bottom: 1px solid #334155;
            padding: 16px 32px;
            display: flex; justify-content: space-between; align-items: center;
        }
        .header h1 { font-size: 20px; color: #38bdf8; }
        .header .refresh { color: #64748b; font-size: 13px; }
        .header button {
            background: #2563eb; color: white; border: none;
            padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px;
        }
        .header button:hover { background: #3b82f6; }
        .container { padding: 24px 32px; max-width: 1400px; margin: 0 auto; }

        /* Stats Cards */
        .stats-grid {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px; margin-bottom: 24px;
        }
        .stat-card {
            background: #1e293b; border: 1px solid #334155; border-radius: 10px;
            padding: 20px; text-align: center;
        }
        .stat-card .number { font-size: 32px; font-weight: 700; color: #38bdf8; }
        .stat-card .label { font-size: 12px; color: #94a3b8; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
        .stat-card.green .number { color: #4ade80; }
        .stat-card.yellow .number { color: #facc15; }
        .stat-card.red .number { color: #f87171; }
        .stat-card.purple .number { color: #a78bfa; }

        /* Sections */
        .section { margin-bottom: 24px; }
        .section-title {
            font-size: 16px; font-weight: 600; color: #94a3b8;
            margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px;
        }

        /* Two column layout */
        .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }
        @media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }

        /* Tables */
        .panel {
            background: #1e293b; border: 1px solid #334155; border-radius: 10px;
            overflow: hidden;
        }
        .panel-title {
            padding: 14px 18px; font-size: 14px; font-weight: 600;
            border-bottom: 1px solid #334155; color: #cbd5e1;
        }
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th {
            text-align: left; padding: 10px 14px; background: #1e293b;
            color: #64748b; font-weight: 600; font-size: 11px;
            text-transform: uppercase; letter-spacing: 0.5px;
            border-bottom: 1px solid #334155;
        }
        td {
            padding: 10px 14px; border-bottom: 1px solid #1e293b;
            color: #cbd5e1; max-width: 300px; overflow: hidden;
            text-overflow: ellipsis; white-space: nowrap;
        }
        tr:hover td { background: #1e293b; }

        /* Badges */
        .badge {
            display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 11px; font-weight: 600;
        }
        .badge-success { background: #166534; color: #4ade80; }
        .badge-error { background: #7f1d1d; color: #fca5a5; }
        .badge-open { background: #1e3a5f; color: #38bdf8; }
        .badge-closed { background: #3f3f46; color: #a1a1aa; }
        .badge-awarded { background: #365314; color: #a3e635; }

        /* Progress bars */
        .quality-row { display: flex; align-items: center; padding: 8px 14px; border-bottom: 1px solid #1e293b; }
        .quality-label { width: 160px; font-size: 13px; color: #94a3b8; }
        .quality-bar-bg {
            flex: 1; height: 8px; background: #334155; border-radius: 4px; overflow: hidden;
        }
        .quality-bar {
            height: 100%; border-radius: 4px; transition: width 0.5s;
        }
        .quality-pct { width: 50px; text-align: right; font-size: 13px; font-weight: 600; }
        .pct-good { color: #4ade80; }
        .pct-ok { color: #facc15; }
        .pct-bad { color: #f87171; }

        /* Loading */
        .loading { text-align: center; padding: 40px; color: #64748b; }
        .error-text { color: #f87171; font-size: 13px; padding: 14px; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Tender DAAS — Dashboard</h1>
        <div>
            <span class="refresh" id="lastUpdate"></span>
            <button onclick="loadAll()">Refresh</button>
        </div>
    </div>

    <div class="container">
        <!-- Stats Cards -->
        <div class="stats-grid" id="statsCards">
            <div class="stat-card"><div class="number">—</div><div class="label">Loading...</div></div>
        </div>

        <!-- Two Column: Status + Category -->
        <div class="two-col">
            <div class="panel">
                <div class="panel-title">Tenders by Status</div>
                <div id="statusTable"><div class="loading">Loading...</div></div>
            </div>
            <div class="panel">
                <div class="panel-title">Tenders by Category</div>
                <div id="categoryTable"><div class="loading">Loading...</div></div>
            </div>
        </div>

        <!-- Data Quality -->
        <div class="section">
            <div class="panel">
                <div class="panel-title">Data Quality</div>
                <div id="qualityPanel"><div class="loading">Loading...</div></div>
            </div>
        </div>

        <!-- Scraper Runs -->
        <div class="section">
            <div class="panel">
                <div class="panel-title">Recent Scraper Runs</div>
                <div id="runsTable"><div class="loading">Loading...</div></div>
            </div>
        </div>

        <!-- Errors -->
        <div class="section">
            <div class="panel">
                <div class="panel-title">Recent Errors</div>
                <div id="errorsPanel"><div class="loading">Loading...</div></div>
            </div>
        </div>

        <!-- Recent Tenders -->
        <div class="section">
            <div class="panel">
                <div class="panel-title">Recently Added Tenders</div>
                <div style="overflow-x: auto;" id="tendersTable"><div class="loading">Loading...</div></div>
            </div>
        </div>
    </div>

    <script>
        async function fetchJSON(url) {
            const res = await fetch(url);
            return await res.json();
        }

        function statusBadge(status) {
            const cls = {open:'badge-open', closed:'badge-closed', awarded:'badge-awarded',
                         success:'badge-success', error:'badge-error'}[status] || 'badge-open';
            return `<span class="badge ${cls}">${status}</span>`;
        }

        function formatDate(iso) {
            if (!iso) return '—';
            const d = new Date(iso);
            return d.toLocaleDateString('en-IN', {day:'2-digit', month:'short', year:'numeric'});
        }

        function formatINR(amount) {
            if (!amount) return '—';
            if (amount >= 10000000) return '₹' + (amount/10000000).toFixed(2) + ' Cr';
            if (amount >= 100000) return '₹' + (amount/100000).toFixed(2) + ' L';
            return '₹' + amount.toLocaleString('en-IN');
        }

        async function loadStats() {
            try {
                const data = await fetchJSON('/api/stats');
                const cards = [
                    {n: data.total_tenders, l: 'Total Tenders', c: ''},
                    {n: data.by_status.find(s=>s.status==='open')?.count || 0, l: 'Live / Open', c: 'green'},
                    {n: data.by_status.find(s=>s.status==='closed')?.count || 0, l: 'Archived', c: 'yellow'},
                    {n: data.by_status.find(s=>s.status==='awarded')?.count || 0, l: 'Awarded', c: 'purple'},
                    {n: data.with_emd, l: 'With EMD Data', c: 'green'},
                    {n: data.with_documents, l: 'With Documents', c: 'green'},
                    {n: data.total_document_urls, l: 'Total Doc URLs', c: ''},
                    {n: data.raw_records, l: 'Raw Records', c: ''},
                ];
                document.getElementById('statsCards').innerHTML = cards.map(c =>
                    `<div class="stat-card ${c.c}"><div class="number">${c.n}</div><div class="label">${c.l}</div></div>`
                ).join('');

                // Status table
                document.getElementById('statusTable').innerHTML = '<table>' +
                    data.by_status.map(s => `<tr><td>${statusBadge(s.status)}</td><td style="text-align:right;font-weight:600">${s.count}</td></tr>`).join('') +
                    '</table>';

                // Category table
                document.getElementById('categoryTable').innerHTML = '<table>' +
                    data.by_category.map(c => `<tr><td>${c.category}</td><td style="text-align:right;font-weight:600">${c.count}</td></tr>`).join('') +
                    '</table>';
            } catch(e) {
                document.getElementById('statsCards').innerHTML = `<div class="error-text">Error loading stats: ${e}</div>`;
            }
        }

        async function loadRuns() {
            try {
                const data = await fetchJSON('/api/runs');
                if (!data.runs.length) {
                    document.getElementById('runsTable').innerHTML = '<div class="loading">No runs yet</div>';
                    return;
                }
                document.getElementById('runsTable').innerHTML = `<table>
                    <tr><th>Portal</th><th>Status</th><th>Found</th><th>New</th><th>Duration</th><th>Time</th><th>Error</th></tr>
                    ${data.runs.map(r => `<tr>
                        <td>${r.portal.toUpperCase()}</td>
                        <td>${statusBadge(r.status)}</td>
                        <td>${r.records_found || 0}</td>
                        <td>${r.records_new || 0}</td>
                        <td>${r.duration_sec ? r.duration_sec + 's' : '—'}</td>
                        <td>${formatDate(r.started_at)}</td>
                        <td title="${r.error || ''}">${r.error ? r.error.substring(0,60)+'...' : '—'}</td>
                    </tr>`).join('')}
                </table>`;
            } catch(e) {
                document.getElementById('runsTable').innerHTML = `<div class="error-text">Error: ${e}</div>`;
            }
        }

        async function loadErrors() {
            try {
                const data = await fetchJSON('/api/errors');
                const all = [...data.run_errors.map(e=>({...e,type:'Run'})), ...data.normalization_errors.map(e=>({...e,type:'Norm'}))];
                if (!all.length) {
                    document.getElementById('errorsPanel').innerHTML = '<div style="padding:14px;color:#4ade80">No errors found</div>';
                    return;
                }
                document.getElementById('errorsPanel').innerHTML = `<table>
                    <tr><th>Type</th><th>Portal</th><th>Error</th><th>Time</th></tr>
                    ${all.slice(0,10).map(e => `<tr>
                        <td><span class="badge badge-error">${e.type}</span></td>
                        <td>${(e.portal||'').toUpperCase()}</td>
                        <td title="${e.error||''}">${(e.error||'').substring(0,80)}</td>
                        <td>${formatDate(e.time)}</td>
                    </tr>`).join('')}
                </table>`;
            } catch(e) {
                document.getElementById('errorsPanel').innerHTML = `<div class="error-text">Error: ${e}</div>`;
            }
        }

        async function loadQuality() {
            try {
                const data = await fetchJSON('/api/quality');
                if (data.total === 0) {
                    document.getElementById('qualityPanel').innerHTML = '<div class="loading">No data yet</div>';
                    return;
                }
                const checks = data.checks;
                let html = '';
                for (const [label, val] of Object.entries(checks)) {
                    if (label === 'total') continue;
                    const pct = val.pct;
                    const color = pct >= 80 ? '#4ade80' : pct >= 50 ? '#facc15' : '#f87171';
                    const pctClass = pct >= 80 ? 'pct-good' : pct >= 50 ? 'pct-ok' : 'pct-bad';
                    html += `<div class="quality-row">
                        <div class="quality-label">${label}</div>
                        <div class="quality-bar-bg"><div class="quality-bar" style="width:${pct}%;background:${color}"></div></div>
                        <div class="quality-pct ${pctClass}">${pct}%</div>
                    </div>`;
                }
                document.getElementById('qualityPanel').innerHTML = html;
            } catch(e) {
                document.getElementById('qualityPanel').innerHTML = `<div class="error-text">Error: ${e}</div>`;
            }
        }

        async function loadTenders() {
            try {
                const data = await fetchJSON('/api/recent-tenders');
                if (!data.tenders.length) {
                    document.getElementById('tendersTable').innerHTML = '<div class="loading">No tenders yet</div>';
                    return;
                }
                document.getElementById('tendersTable').innerHTML = `<table>
                    <tr><th>Ref</th><th>Title</th><th>Status</th><th>Category</th><th>EMD</th><th>Published</th><th>Deadline</th><th>Docs</th></tr>
                    ${data.tenders.map(t => `<tr>
                        <td>${t.ref || '—'}</td>
                        <td title="${t.title}">${t.title.substring(0,50)}${t.title.length>50?'...':''}</td>
                        <td>${statusBadge(t.status)}</td>
                        <td>${t.category || '—'}</td>
                        <td>${formatINR(t.emd)}</td>
                        <td>${formatDate(t.published)}</td>
                        <td>${formatDate(t.deadline)}</td>
                        <td>${t.docs || 0}</td>
                    </tr>`).join('')}
                </table>`;
            } catch(e) {
                document.getElementById('tendersTable').innerHTML = `<div class="error-text">Error: ${e}</div>`;
            }
        }

        function loadAll() {
            document.getElementById('lastUpdate').textContent = 'Updated: ' + new Date().toLocaleTimeString();
            loadStats();
            loadRuns();
            loadErrors();
            loadQuality();
            loadTenders();
        }

        // Load on page open
        loadAll();

        // Auto-refresh every 30 seconds
        setInterval(loadAll, 30000);
    </script>
</body>
</html>
"""
