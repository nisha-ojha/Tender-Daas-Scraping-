"""
core/db.py
----------
All database operations live here. Every portal uses these functions.
No portal should talk to the database directly — always go through this file.

WHY: If you ever switch from PostgreSQL to something else, you only change this one file.
"""

import os
import json
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv

# Load .env file (reads DATABASE_HOST, DATABASE_PASSWORD, etc.)
load_dotenv()

# Database configuration — pulled from .env, NOT hardcoded
DATABASE = {
    "host": os.getenv("DATABASE_HOST", "localhost"),
    "port": int(os.getenv("DATABASE_PORT", 5432)),
    "dbname": os.getenv("DATABASE_NAME", "tender_db"),
    "user": os.getenv("DATABASE_USER", "tender_user"),
    "password": os.getenv("DATABASE_PASSWORD"),
}


def get_connection():
    """
    Get a database connection.
    
    Returns a psycopg2 connection object.
    The caller is responsible for closing it (use try/finally or 'with').
    """
    try:
        conn = psycopg2.connect(
            host=DATABASE["host"],
            port=DATABASE["port"],
            dbname=DATABASE["dbname"],
            user=DATABASE["user"],
            password=DATABASE["password"],
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"[DB ERROR] Cannot connect to PostgreSQL: {e}")
        print(f"[DB ERROR] Check: Is PostgreSQL running? Is the password in .env correct?")
        raise


def insert_raw_record(conn, portal, raw_data, batch_id, html_snapshot=None):
    """
    Save a raw scraped record to raw_records table.
    
    IMPORTANT: This uses the connection passed to it (does NOT commit).
    The pipeline.py handles commit/rollback for the entire batch.
    
    Args:
        conn: Database connection (from pipeline's transaction)
        portal: 'seci', 'cppp', etc.
        raw_data: Dictionary of scraped data
        batch_id: Unique ID for this pipeline run
        html_snapshot: Full HTML of the page (optional, for debugging)
    
    Returns:
        The ID of the inserted record, or None on failure
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO raw_records (portal, raw_data, html_snapshot, batch_id)
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (portal, json.dumps(raw_data), html_snapshot, batch_id),
        )
        record_id = cur.fetchone()[0]
        return record_id
    except Exception as e:
        print(f"[DB ERROR] Failed to insert raw record: {e}")
        raise  # Let pipeline.py handle the rollback
    finally:
        cur.close()


def insert_tender(conn, tender_data):
    """
    Insert a normalized tender into the main tenders table.
    
    Uses ON CONFLICT to skip exact duplicates (same reference_number + org).
    
    Args:
        conn: Database connection (from pipeline's transaction)
        tender_data: Dictionary matching tenders table columns
    
    Returns:
        The UUID of the inserted tender, or None if it was a duplicate
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO tenders (
                reference_number, title, title_clean,
                organization, organization_short, department,
                value, value_display, emd_amount,
                date_published, deadline, bid_opening_date,
                category, subcategory, tender_type,
                state, district, niche_metadata,
                document_urls, document_count,
                source_portal, source_url, all_sources,
                status, hash, batch_id
            ) VALUES (
                %(reference_number)s, %(title)s, %(title_clean)s,
                %(organization)s, %(organization_short)s, %(department)s,
                %(value)s, %(value_display)s, %(emd_amount)s,
                %(date_published)s, %(deadline)s, %(bid_opening_date)s,
                %(category)s, %(subcategory)s, %(tender_type)s,
                %(state)s, %(district)s, %(niche_metadata)s,
                %(document_urls)s, %(document_count)s,
                %(source_portal)s, %(source_url)s, %(all_sources)s,
                %(status)s, %(hash)s, %(batch_id)s
            )
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """,
            tender_data,
        )
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"[DB ERROR] Failed to insert tender: {e}")
        raise
    finally:
        cur.close()


def get_unprocessed_raw_records(conn, portal, batch_id):
    """
    Fetch raw records that haven't been normalized yet.
    Used by the normalizer stage.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT id, raw_data FROM raw_records
               WHERE portal = %s AND batch_id = %s AND processed = FALSE
               ORDER BY id""",
            (portal, batch_id),
        )
        return cur.fetchall()
    finally:
        cur.close()


def mark_raw_record_processed(conn, record_id, error_message=None):
    """Mark a raw record as processed (or failed with error)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE raw_records SET processed = TRUE, error_message = %s
               WHERE id = %s""",
            (error_message, record_id),
        )
    finally:
        cur.close()


def find_by_reference(conn, ref_number, org_short):
    """Find an existing tender by reference number + organization."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """SELECT * FROM tenders
               WHERE reference_number = %s AND organization_short = %s""",
            (ref_number, org_short),
        )
        return cur.fetchone()
    finally:
        cur.close()


def log_scraper_run(conn, portal, batch_id, status, records_found=0,
                     records_new=0, records_updated=0, error_message=None):
    """
    Log a pipeline run to scraper_runs table.
    This is called at the END of a pipeline run (success or failure).
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO scraper_runs
                (portal, batch_id, status, records_found, records_new,
                 records_updated, error_message, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (portal, batch_id, status, records_found, records_new,
             records_updated, error_message),
        )
        conn.commit()  # This one commits independently — it's a log, not data
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] Failed to log scraper run: {e}")
    finally:
        cur.close()


# ─── Quick self-test ─────────────────────────────────────────
# Run this file directly to check if the database connection works:
#   python core/db.py

if __name__ == "__main__":
    print("Testing database connection...")
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Check if tables exist
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]

        if tables:
            print(f"✓ Connected to '{DATABASE['dbname']}' on {DATABASE['host']}")
            print(f"✓ Found {len(tables)} tables: {', '.join(tables)}")

            # Count tenders if the table exists
            if "tenders" in tables:
                cur.execute("SELECT COUNT(*) FROM tenders")
                count = cur.fetchone()[0]
                print(f"✓ Tenders in database: {count}")
        else:
            print(f"✓ Connected to '{DATABASE['dbname']}' but NO TABLES found.")
            print("  You need to create tables first. See Step 3 below.")

        cur.close()
        conn.close()
        print("\n✓ Database connection test PASSED!")

    except Exception as e:
        print(f"\n✗ Database connection test FAILED: {e}")
        print("\nTroubleshooting:")
        print("  1. Is PostgreSQL running? Open Services (Win+R → services.msc)")
        print("  2. Is the password correct in .env?")
        print("  3. Does the database 'tender_db' exist?")
        print("  4. Does the user 'tender_user' exist?")
