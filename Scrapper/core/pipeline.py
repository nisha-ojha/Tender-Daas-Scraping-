"""
core/pipeline.py
----------------
Pipeline runner: Scrape → Normalize → PDF Download → PDF Parse → Dedup
"""

import importlib
import traceback
from datetime import datetime

from core.db import get_connection, log_scraper_run


def run_pipeline(portal: str, batch_id: str, stages: str = "all") -> dict:

    print(f"\n{'='*60}")
    print(f"  PIPELINE START: {portal.upper()}")
    print(f"  Batch:  {batch_id}")
    print(f"  Stages: {stages}")
    print(f"  Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    result = {
        "new": 0,
        "updated": 0,
        "errors": 0,
        "raw_records": 0,
        "pdfs_downloaded": 0,
        "pdfs_parsed": 0,
    }

    # ── Load portal modules ─────────────────────────────────────
    try:
        scraper_mod = importlib.import_module(f"portals.{portal}.scraper")
        normalizer_mod = importlib.import_module(f"portals.{portal}.normalizer")
    except ModuleNotFoundError as e:
        raise Exception(f"Portal '{portal}' not found: {e}")

    conn = get_connection()

    try:
        conn.autocommit = False

        # ============================================================
        # STAGE 1: SCRAPE
        # ============================================================
        if stages in ("all", "scrape"):
            print(f"  ── Stage 1: SCRAPE ({portal.upper()}) ──")
            raw_count = scraper_mod.scrape(conn=conn, batch_id=batch_id)
            result["raw_records"] = raw_count
            print(f"  ── DONE: {raw_count} raw records ──\n")

        # ============================================================
        # STAGE 2: NORMALIZE
        # ============================================================
        if stages in ("all", "normalize"):
            print(f"  ── Stage 2: NORMALIZE ({portal.upper()}) ──")
            norm_result = normalizer_mod.normalize(conn=conn, batch_id=batch_id)
            result["new"] = norm_result.get("new", 0)
            result["errors"] = norm_result.get("errors", 0)
            print(f"  ── DONE: {result['new']} tenders ──\n")

        # ============================================================
        # STAGE 3: PDF DOWNLOAD
        # ============================================================
        if stages in ("all", "pdf"):
            print(f"  ── Stage 3: PDF DOWNLOAD ({portal.upper()}) ──")

            from core.pdf.downloader import run_download_stage

            pdf_result = run_download_stage(conn, batch_id)
            result["pdfs_downloaded"] = pdf_result.get("downloaded", 0)

            print(f"  ── DONE: {result['pdfs_downloaded']} PDFs downloaded ──\n")

        # ============================================================
        # STAGE 4: PDF PARSE
        # ============================================================
        if stages in ("all", "pdf"):
            print(f"  ── Stage 4: PDF PARSE ({portal.upper()}) ──")

            from core.pdf.parser_rfs import parse_rfs_pdf, apply_rfs_data_to_tender
            from core.pdf.classifier import classify_pdf_file
            import psycopg2.extras

            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            cur.execute("""
                SELECT id, tender_id, local_path, doc_name
                FROM tender_documents
                WHERE downloaded = TRUE
                  AND parsed = FALSE
                LIMIT 50
            """)

            docs = cur.fetchall()
            cur.close()

            parsed_count = 0

            for doc in docs:
                if not doc["local_path"]:
                    continue

                doc_type = classify_pdf_file(doc["local_path"], doc["doc_name"])

                if doc_type in ("RfS", "RfP", "NIT"):
                    data = parse_rfs_pdf(doc["local_path"])

                    if data and "_parse_error" not in data:
                        apply_rfs_data_to_tender(conn, doc["tender_id"], data)
                        parsed_count += 1

                cur2 = conn.cursor()
                cur2.execute(
                    "UPDATE tender_documents SET parsed=TRUE, parsed_at=NOW() WHERE id=%s",
                    (doc["id"],)
                )
                cur2.close()

            result["pdfs_parsed"] = parsed_count

            print(f"  ── DONE: {parsed_count} PDFs parsed ──\n")

        # ============================================================
        # STAGE 5: DEDUP (placeholder)
        # ============================================================
        if stages in ("all", "dedup"):
            print(f"  ── Stage 5: DEDUP (not built yet) ──\n")

        # ============================================================
        # COMMIT
        # ============================================================
        conn.commit()
        print("  ✓ COMMITTED")

        # Log success
        log_conn = get_connection()
        try:
            log_scraper_run(
                conn=log_conn,
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
        conn.rollback()

        print("\n  ✗ ROLLED BACK")
        print(f"  Error: {e}")
        print(traceback.format_exc())

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
            print("  [WARNING] Failed to log error")

        raise

    finally:
        conn.close()

        print(f"\n{'='*60}")
        print(f"  PIPELINE END: {portal.upper()}")
        print(f"{'='*60}\n")