"""
core/pdf/downloader.py
----------------------
Downloads PDF documents from tender detail pages.
Saves them to storage/pdfs/{portal}/{reference_number}/

HOW IT WORKS:
  1. Gets all tenders that have document_urls but pdfs_downloaded = FALSE
  2. For each document URL, downloads the PDF
  3. Saves to local disk
  4. Inserts a row into tender_documents table
  5. Updates tenders.pdfs_downloaded = TRUE when done

RUN STANDALONE (test):
  python core/pdf/downloader.py
"""

import os
import sys
import re
import time
import hashlib
import requests
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from core.db import get_connection

# ── Config ────────────────────────────────────────────────────
STORAGE_DIR   = "storage/pdfs"
TIMEOUT_SEC   = 30
RETRY_COUNT   = 3
RATE_LIMIT    = 2.0          # seconds between downloads
MAX_FILE_MB   = 2           # skip files larger than this

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )
}


# ══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

def run_download_stage(conn, batch_id: str) -> dict:
    """
    Called by pipeline.py as Stage 3.
    Downloads all PDFs for tenders in this batch.

    Returns:
        {"downloaded": int, "skipped": int, "failed": int}
    """
    result = {"downloaded": 0, "skipped": 0, "failed": 0}

    # Get tenders from this batch that have documents
    tenders = _get_tenders_with_docs(conn, batch_id)
    tenders = tenders[:2]
    print(f"  Found {len(tenders)} tenders with documents to download")

    for tender in tenders:
        tender_id      = tender["id"]
        ref            = tender["reference_number"] or f"no_ref_{tender_id}"
        portal         = tender["source_portal"]
        doc_urls       = tender["document_urls"] or []
        niche_meta     = tender["niche_metadata"] or {}

        # Build full document list from both document_urls and niche_metadata
        all_docs = _build_doc_list(doc_urls, niche_meta)

        if not all_docs:
            result["skipped"] += 1
            continue

        print(f"\n  Downloading docs for: {ref} ({len(all_docs)} files)")

        tender_downloaded = 0
        for doc in all_docs:
            url      = doc.get("url", "")
            doc_name = doc.get("name", "document")
            uploaded = doc.get("uploaded")

            if not url or not url.startswith("http"):
                continue

            # Check if already downloaded
            if _already_downloaded(conn, tender_id, url):
                result["skipped"] += 1
                continue

            # Download
            local_path, error = _download_file(url, portal, ref)

            # Insert into tender_documents
            _insert_document_record(
                conn=conn,
                tender_id=tender_id,
                ref=ref,
                portal=portal,
                doc_name=doc_name,
                doc_url=url,
                uploaded_date=uploaded,
                local_path=local_path,
                error=error,
                batch_id=batch_id,
            )

            if local_path:
                result["downloaded"] += 1
                tender_downloaded += 1
                print(f"    ✓ {doc_name[:60]}")
            else:
                result["failed"] += 1
                print(f"    ✗ {doc_name[:60]} — {error}")

            time.sleep(RATE_LIMIT)

        # Mark tender as having PDFs downloaded
        if tender_downloaded > 0:
            _mark_pdfs_downloaded(conn, tender_id)

    print(f"\n  Download complete: {result['downloaded']} downloaded, "
          f"{result['skipped']} skipped, {result['failed']} failed")
    return result


# ══════════════════════════════════════════════════════════════
# DOWNLOAD A SINGLE FILE
# ══════════════════════════════════════════════════════════════

def _download_file(url: str, portal: str, ref: str) -> tuple[str | None, str | None]:
    """
    Download one PDF file to disk.

    Returns:
        (local_path, None)      on success
        (None, error_message)   on failure
    """
    # Build save directory
    safe_ref = re.sub(r"[^\w\-]", "_", ref)[:80]
    save_dir = Path(STORAGE_DIR) / portal / safe_ref
    save_dir.mkdir(parents=True, exist_ok=True)

    # Build filename from URL
    url_filename = url.split("/")[-1].split("?")[0]
    if not url_filename.lower().endswith(".pdf"):
        # Hash the URL to create unique name
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        url_filename = f"doc_{url_hash}.pdf"

    # Sanitise filename
    safe_name = re.sub(r"[^\w\-.]", "_", url_filename)[:100]
    local_path = save_dir / safe_name

    # Skip if file already exists on disk
    if local_path.exists() and local_path.stat().st_size > 1000:
        return str(local_path), None

    # Download with retries
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT_SEC,
                stream=True,
                allow_redirects=True,
            )

            if response.status_code != 200:
                if attempt == RETRY_COUNT - 1:
                    return None, f"HTTP {response.status_code}"
                time.sleep(2 ** attempt)
                continue

            # Check content type
            content_type = response.headers.get("Content-Type", "")
            if "html" in content_type and "pdf" not in content_type:
                return None, "Response is HTML not PDF (login wall?)"

            # Check file size
            content_length = int(response.headers.get("Content-Length", 0))
            if content_length > MAX_FILE_MB * 1024 * 1024:
                return None, f"File too large ({content_length // 1024 // 1024}MB)"

            # Write to disk
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            # Verify it's actually a PDF
            with open(local_path, "rb") as f:
                header = f.read(4)
            if header != b"%PDF":
                local_path.unlink(missing_ok=True)
                return None, "Downloaded file is not a valid PDF"

            return str(local_path), None

        except requests.exceptions.Timeout:
            if attempt == RETRY_COUNT - 1:
                return None, "Timeout after retries"
            time.sleep(2 ** attempt)

        except requests.exceptions.ConnectionError as e:
            if attempt == RETRY_COUNT - 1:
                return None, f"Connection error: {str(e)[:80]}"
            time.sleep(2 ** attempt)

        except Exception as e:
            return None, str(e)[:100]

    return None, "All retries failed"


# ══════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════
def _get_tenders_with_docs(conn, batch_id: str) -> list:
    import psycopg2.extras

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT id, reference_number, source_portal,
                   document_urls, niche_metadata, status
            FROM tenders
            WHERE pdfs_downloaded = FALSE
              AND (
                  document_count > 0
                  OR document_urls IS NOT NULL
                  OR niche_metadata ? 'documents'
              )
            ORDER BY
                CASE status
                    WHEN 'open'    THEN 1
                    WHEN 'closed'  THEN 2
                    WHEN 'awarded' THEN 3
                    ELSE 4
                END,
                created_at DESC
            LIMIT 100
        """)
        return cur.fetchall()
    finally:
        cur.close()


def _already_downloaded(conn, tender_id, url: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM tender_documents WHERE tender_id = %s AND doc_url = %s",
            (tender_id, url)
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _insert_document_record(conn, tender_id, ref, portal, doc_name,
                             doc_url, uploaded_date, local_path, error, batch_id):
    import json
    cur = conn.cursor()
    try:
        # Classify document type from name
        from core.pdf.classifier import classify_by_name
        doc_type = classify_by_name(doc_name)

        cur.execute("""
            INSERT INTO tender_documents
                (tender_id, reference_number, portal, doc_name, doc_url,
                 doc_type, uploaded_date, downloaded, downloaded_at,
                 local_path, parse_error, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            tender_id, ref, portal, doc_name[:200], doc_url,
            doc_type, uploaded_date,
            local_path is not None,
            datetime.utcnow() if local_path else None,
            local_path, error, batch_id,
        ))
    finally:
        cur.close()


def _mark_pdfs_downloaded(conn, tender_id):
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE tenders SET pdfs_downloaded = TRUE WHERE id = %s",
            (tender_id,)
        )
    finally:
        cur.close()


def _build_doc_list(document_urls: list, niche_metadata: dict) -> list:
    """
    Combine document_urls array and niche_metadata documents
    into one list of {name, url} dicts.
    """
    docs = []

    # From document_urls (plain URL strings)
    if document_urls:
        for url in document_urls:
            if isinstance(url, str) and url.startswith("http"):
                docs.append({"name": url.split("/")[-1], "url": url})

    # From niche_metadata.documents (list of {name, url})
    if isinstance(niche_metadata, dict):
        meta_docs = niche_metadata.get("documents", [])
        if isinstance(meta_docs, list):
            for d in meta_docs:
                if isinstance(d, dict) and d.get("url"):
                    docs.append(d)

        # Also check announcements (corrigenda)
        announcements = niche_metadata.get("announcements", [])
        if isinstance(announcements, list):
            for d in announcements:
                if isinstance(d, dict) and d.get("url"):
                    docs.append(d)

    # Deduplicate by URL
    seen = set()
    unique = []
    for d in docs:
        url = d.get("url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(d)

    return unique


# ── Standalone test ───────────────────────────────────────────
if __name__ == "__main__":
    print("Testing PDF downloader...")
    conn = get_connection()
    conn.autocommit = True

    # Get most recent batch
    cur = conn.cursor()
    cur.execute("SELECT batch_id FROM scraper_runs ORDER BY started_at DESC LIMIT 1")
    row = cur.fetchone()
    cur.close()

    if not row:
        print("No scraper runs found. Run the scraper first.")
    else:
        batch_id = row[0]
        print(f"Testing with batch: {batch_id}")
        result = run_download_stage(conn, batch_id)
        print(f"\nResult: {result}")

    conn.close()