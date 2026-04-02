"""
core/db.py
----------
All database operations for the normalized schema v3.

SCHEMA STRUCTURE:
  tenders          → core identity (portal lives here)
  tender_details   → dates, status, location
  tender_financial → EMD, fees, tariff, SD, PG
  tender_technical → capacity, eligibility, bid system
  tender_documents → PDFs and attachments
  tender_bidders   → L1/L2/L3 bidder data
  tender_awards    → awarded contractor info
  raw_records      → scraper safety net
  scraper_runs     → pipeline execution log

RULE: Every portal uses these functions.
      No portal file should talk to DB directly.
      If you switch from PostgreSQL, change only this file.
"""

import os
import json
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE = {
    "host":     os.getenv("DATABASE_HOST", "localhost"),
    "port":     int(os.getenv("DATABASE_PORT", 5432)),
    "dbname":   os.getenv("DATABASE_NAME", "tender_db"),
    "user":     os.getenv("DATABASE_USER", "tender_user"),
    "password": os.getenv("DATABASE_PASSWORD"),
}


# ══════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════

def get_connection():
    """
    Get a PostgreSQL database connection.
    Caller is responsible for closing it (use try/finally).
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
        print(f"[DB ERROR] Check .env — is PostgreSQL running?")
        raise


# ══════════════════════════════════════════════════════════════
# RAW RECORDS
# ══════════════════════════════════════════════════════════════

def insert_raw_record(conn, portal, raw_data, batch_id, html_snapshot=None):
    """
    Save a raw scraped record to raw_records table.
    Does NOT commit — pipeline.py handles the transaction.

    Returns the row ID, or None on failure.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO raw_records (portal, raw_data, html_snapshot, batch_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (portal, json.dumps(raw_data), html_snapshot, batch_id),
        )
        return cur.fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] Failed to insert raw record: {e}")
        raise
    finally:
        cur.close()


def get_unprocessed_raw_records(conn, portal, batch_id):
    """
    Fetch raw records not yet normalized.
    Called by the normalizer stage.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """
            SELECT id, raw_data FROM raw_records
            WHERE portal = %s AND batch_id = %s AND processed = FALSE
            ORDER BY id
            """,
            (portal, batch_id),
        )
        return cur.fetchall()
    finally:
        cur.close()


def mark_raw_record_processed(conn, record_id, error_message=None):
    """Mark a raw record as processed (success or failure)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE raw_records
            SET processed = TRUE, error_message = %s
            WHERE id = %s
            """,
            (error_message, record_id),
        )
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# TENDERS — normalized insert across 4 tables
# ══════════════════════════════════════════════════════════════

def _to_pg_text_array(lst):
    """
    Convert a Python list of strings to a PostgreSQL TEXT[] literal.
    psycopg2 may JSON-serialize lists instead of using array syntax,
    so we format it explicitly: ["a","b"] → '{"a","b"}'
    """
    if not lst:
        return None
    items = ",".join(
        '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
        for s in lst if s
    )
    return "{" + items + "}"


def insert_tender(conn, tender_data: dict):
    """
    Insert a normalized tender into the split table structure.

    Writes to:
      1. tenders          — core identity
      2. tender_details   — dates, status, location
      3. tender_financial — EMD, fees, tariff
      4. tender_technical — capacity, eligibility, bid system

    Args:
        conn:        DB connection (from pipeline transaction)
        tender_data: Dict with all tender fields

    Returns:
        UUID of the inserted tender, or None if it was a duplicate.
    """
    if not tender_data.get("reference_number"):
        return None

    cur = conn.cursor()
    try:

        # ── Step 1: tenders ──────────────────────────────────────
        cur.execute(
            """
            INSERT INTO tenders (
                portal,
                reference_number,
                cppp_tender_id,
                title,
                title_clean,
                description,
                organization,
                organization_short,
                department,
                ministry,
                category,
                subcategory,
                tender_type,
                procurement_type,
                source_url,
                detail_url,
                all_sources,
                batch_id,
                hash
            ) VALUES (
                %(portal)s,
                %(reference_number)s,
                %(cppp_tender_id)s,
                %(title)s,
                %(title_clean)s,
                %(description)s,
                %(organization)s,
                %(organization_short)s,
                %(department)s,
                %(ministry)s,
                %(category)s,
                %(subcategory)s,
                %(tender_type)s,
                %(procurement_type)s,
                %(source_url)s,
                %(detail_url)s,
                %(all_sources)s,
                %(batch_id)s,
                %(hash)s
            )
            ON CONFLICT (reference_number, portal)
            WHERE reference_number IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            {
                "portal":             tender_data.get("portal"),
                "reference_number":   tender_data.get("reference_number"),
                "cppp_tender_id":     tender_data.get("cppp_tender_id"),
                "title":              tender_data.get("title"),
                "title_clean":        tender_data.get("title_clean"),
                "description":        tender_data.get("description"),
                "organization":       tender_data.get("organization"),
                "organization_short": tender_data.get("organization_short"),
                "department":         tender_data.get("department"),
                "ministry":           tender_data.get("ministry"),
                "category":           tender_data.get("category", "Uncategorized"),
                "subcategory":        tender_data.get("subcategory"),
                "tender_type":        tender_data.get("tender_type"),
                "procurement_type":   tender_data.get("procurement_type"),
                "source_url":         tender_data.get("source_url"),
                "detail_url":         tender_data.get("detail_url"),
                "all_sources":        _to_pg_text_array(tender_data.get("all_sources")),
                "batch_id":           tender_data.get("batch_id"),
                "hash":               tender_data.get("hash"),
            }
        )

        result = cur.fetchone()
        if not result:
            return None     # Duplicate — already in DB
        tender_id = result[0]

        # ── Step 2: tender_details ───────────────────────────────
        cur.execute(
            """
            INSERT INTO tender_details (
                tender_id,
                reference_number,
                status,
                date_published,
                pre_bid_date,
                bid_submission_online,
                bid_submission_offline,
                deadline,
                bid_opening_date,
                financial_bid_opening,
                notification_number,
                corrigendum_count,
                state,
                district,
                region,
                location_text,
                extra_data
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                tender_id,
                tender_data.get("reference_number"),
                tender_data.get("status", "open"),
                tender_data.get("date_published"),
                tender_data.get("pre_bid_date"),
                tender_data.get("bid_submission_online"),
                tender_data.get("bid_submission_offline"),
                tender_data.get("deadline"),
                tender_data.get("bid_opening_date"),
                tender_data.get("financial_bid_opening"),
                tender_data.get("notification_number"),
                tender_data.get("corrigendum_count", 0),
                tender_data.get("state"),
                tender_data.get("district"),
                tender_data.get("region"),
                tender_data.get("location_text"),
                json.dumps(tender_data.get("extra_data") or {}),
            )
        )

        # ── Step 3: tender_financial ─────────────────────────────
        cur.execute(
            """
            INSERT INTO tender_financial (
                tender_id,
                reference_number,
                estimated_value,
                estimated_value_display,
                value_is_estimated,
                emd_amount,
                emd_raw_text,
                emd_per_mw,
                emd_currency,
                emd_is_formula,
                emd_exemption_msme,
                emd_exemption_startup,
                tender_fee,
                tender_fee_raw,
                tender_fee_refundable,
                processing_fee,
                processing_fee_per_mw,
                processing_fee_raw,
                sd_percentage,
                sd_raw_text,
                sd_form,
                pg_percentage,
                pg_raw_text,
                pg_validity_months,
                pg_form,
                liquidated_damages_pct,
                ld_cap_pct,
                tariff_ceiling,
                tariff_floor,
                l1_tariff,
                tariff_type,
                payment_security,
                mobilisation_advance,
                advance_percentage
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            (
                tender_id,
                tender_data.get("reference_number"),
                tender_data.get("estimated_value"),
                tender_data.get("estimated_value_display"),
                tender_data.get("value_is_estimated", True),
                tender_data.get("emd_amount"),
                tender_data.get("emd_raw_text"),
                tender_data.get("emd_per_mw"),
                tender_data.get("emd_currency", "INR"),
                tender_data.get("emd_is_formula", False),
                tender_data.get("emd_exemption_msme", False),
                tender_data.get("emd_exemption_startup", False),
                tender_data.get("tender_fee"),
                tender_data.get("tender_fee_raw"),
                tender_data.get("tender_fee_refundable", False),
                tender_data.get("processing_fee"),
                tender_data.get("processing_fee_per_mw"),
                tender_data.get("processing_fee_raw"),
                tender_data.get("sd_percentage"),
                tender_data.get("sd_raw_text"),
                tender_data.get("sd_form"),
                tender_data.get("pg_percentage"),
                tender_data.get("pg_raw_text"),
                tender_data.get("pg_validity_months"),
                tender_data.get("pg_form"),
                tender_data.get("liquidated_damages_pct"),
                tender_data.get("ld_cap_pct"),
                tender_data.get("tariff_ceiling"),
                tender_data.get("tariff_floor"),
                tender_data.get("l1_tariff"),
                tender_data.get("tariff_type"),
                tender_data.get("payment_security"),
                tender_data.get("mobilisation_advance", False),
                tender_data.get("advance_percentage"),
            )
        )

        # ── Step 4: tender_technical ─────────────────────────────
        eligibility = tender_data.get("eligibility")
        cur.execute(
            """
            INSERT INTO tender_technical (
                tender_id,
                reference_number,
                capacity_mw,
                capacity_mwh,
                capacity_kw,
                capacity_raw_text,
                power_type,
                energy_storage_required,
                project_model,
                connectivity,
                interconnection_point,
                substation_kv,
                land_acres,
                land_responsibility,
                no_of_covers,
                bid_system_type,
                reverse_auction,
                domestic_content_req,
                is_international,
                contract_type,
                om_period_years,
                ppa_duration_years,
                concession_period_years,
                net_worth_required_cr,
                turnover_required_cr,
                experience_required_mw,
                experience_raw_text,
                consortium_allowed,
                max_consortium_members,
                foreign_bidder_allowed,
                msme_exemption,
                startup_eligible,
                eligibility
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                tender_id,
                tender_data.get("reference_number"),
                tender_data.get("capacity_mw"),
                tender_data.get("capacity_mwh"),
                tender_data.get("capacity_kw"),
                tender_data.get("capacity_raw_text"),
                tender_data.get("power_type"),
                tender_data.get("energy_storage_required"),
                tender_data.get("project_model"),
                tender_data.get("connectivity"),
                tender_data.get("interconnection_point"),
                tender_data.get("substation_kv"),
                tender_data.get("land_acres"),
                tender_data.get("land_responsibility"),
                tender_data.get("no_of_covers"),
                tender_data.get("bid_system_type"),
                tender_data.get("reverse_auction", False),
                tender_data.get("domestic_content_req"),
                tender_data.get("is_international", False),
                tender_data.get("contract_type"),
                tender_data.get("om_period_years"),
                tender_data.get("ppa_duration_years"),
                tender_data.get("concession_period_years"),
                tender_data.get("net_worth_required_cr"),
                tender_data.get("turnover_required_cr"),
                tender_data.get("experience_required_mw"),
                tender_data.get("experience_raw_text"),
                tender_data.get("consortium_allowed"),
                tender_data.get("max_consortium_members"),
                tender_data.get("foreign_bidder_allowed"),
                tender_data.get("msme_exemption"),
                tender_data.get("startup_eligible"),
                json.dumps(eligibility) if eligibility else None,
            )
        )

        return tender_id

    except Exception as e:
        print(f"[DB ERROR] Failed to insert tender: {e}")
        raise
    finally:
        cur.close()


def find_by_reference(conn, ref_number: str, portal: str):
    """
    Find an existing tender by reference number + portal.
    Used to skip duplicates during scraping and normalization.

    Args:
        ref_number: e.g. "SECI/C&P/IPP/13/0020/25-26"
        portal:     e.g. "seci"  (lowercase portal name)

    Returns:
        Row dict with at least 'id', or None if not found.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """
            SELECT id, reference_number, portal
            FROM tenders
            WHERE reference_number = %s AND portal = %s
            """,
            (ref_number, portal),
        )
        return cur.fetchone()
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# TENDER DOCUMENTS
# ══════════════════════════════════════════════════════════════

def insert_tender_document(conn, tender_id: str, doc_data: dict):
    """
    Insert one document record into tender_documents.
    Called by the PDF downloader.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO tender_documents (
                tender_id, reference_number,
                notification_number, doc_name, doc_url,
                doc_type, uploaded_date, is_amendment, amendment_number,
                downloaded, downloaded_at, local_path,
                parse_error, batch_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            """,
            (
                tender_id,
                doc_data.get("reference_number"),
                doc_data.get("notification_number"),
                doc_data.get("doc_name"),
                doc_data.get("doc_url"),
                doc_data.get("doc_type"),
                doc_data.get("uploaded_date"),
                doc_data.get("is_amendment", False),
                doc_data.get("amendment_number"),
                doc_data.get("downloaded", False),
                doc_data.get("downloaded_at"),
                doc_data.get("local_path"),
                doc_data.get("parse_error"),
                doc_data.get("batch_id"),
            )
        )
    except Exception as e:
        print(f"[DB ERROR] Failed to insert document: {e}")
        raise
    finally:
        cur.close()


def get_documents_for_download(conn, limit: int = 100):
    """
    Get tenders that have documents but haven't been downloaded yet.
    Prioritises open tenders first.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            """
            SELECT
                t.id, t.reference_number, t.portal,
                t.source_url,
                d.status,
                t.batch_id
            FROM tenders t
            JOIN tender_details d ON d.tender_id = t.id
            WHERE NOT EXISTS (
                SELECT 1 FROM tender_documents td
                WHERE td.tender_id = t.id AND td.downloaded = TRUE
            )
            ORDER BY
                CASE d.status
                    WHEN 'open'    THEN 1
                    WHEN 'closed'  THEN 2
                    WHEN 'awarded' THEN 3
                    ELSE 4
                END,
                t.created_at DESC
            LIMIT %s
            """,
            (limit,)
        )
        return cur.fetchall()
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# TENDER BIDDERS
# ══════════════════════════════════════════════════════════════

def insert_bidder(conn, tender_id: str, bidder_data: dict):
    """Insert one bidder record into tender_bidders."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO tender_bidders (
                tender_id, reference_number,
                bidder_name, bidder_name_clean,
                bidder_pan, bidder_gst,
                is_consortium, consortium_lead, consortium_members,
                bid_rank, quoted_tariff, quoted_value,
                bid_valid, disqualified_reason, is_winner,
                source_pdf_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                tender_id,
                bidder_data.get("reference_number"),
                bidder_data.get("bidder_name"),
                bidder_data.get("bidder_name_clean"),
                bidder_data.get("bidder_pan"),
                bidder_data.get("bidder_gst"),
                bidder_data.get("is_consortium", False),
                bidder_data.get("consortium_lead"),
                json.dumps(bidder_data.get("consortium_members")) if bidder_data.get("consortium_members") else None,
                bidder_data.get("bid_rank"),
                bidder_data.get("quoted_tariff"),
                bidder_data.get("quoted_value"),
                bidder_data.get("bid_valid", True),
                bidder_data.get("disqualified_reason"),
                bidder_data.get("is_winner", False),
                bidder_data.get("source_pdf_url"),
            )
        )
    except Exception as e:
        print(f"[DB ERROR] Failed to insert bidder: {e}")
        raise
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# TENDER AWARDS
# ══════════════════════════════════════════════════════════════

def insert_award(conn, tender_id: str, award_data: dict):
    """Insert award data into tender_awards."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO tender_awards (
                tender_id, reference_number,
                awarded_to, awarded_to_clean,
                awarded_to_pan, awarded_to_gst,
                awarded_to_address, awarded_to_state,
                is_consortium_award, consortium_members,
                awarded_value, awarded_value_display,
                awarded_tariff, no_of_bids_received,
                loa_date, loa_number, agreement_date,
                loa_pdf_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (tender_id) DO UPDATE SET
                awarded_to            = EXCLUDED.awarded_to,
                awarded_to_clean      = EXCLUDED.awarded_to_clean,
                awarded_value         = EXCLUDED.awarded_value,
                awarded_tariff        = EXCLUDED.awarded_tariff,
                no_of_bids_received   = EXCLUDED.no_of_bids_received,
                loa_date              = EXCLUDED.loa_date
            """,
            (
                tender_id,
                award_data.get("reference_number"),
                award_data.get("awarded_to"),
                award_data.get("awarded_to_clean"),
                award_data.get("awarded_to_pan"),
                award_data.get("awarded_to_gst"),
                award_data.get("awarded_to_address"),
                award_data.get("awarded_to_state"),
                award_data.get("is_consortium_award", False),
                json.dumps(award_data.get("consortium_members")) if award_data.get("consortium_members") else None,
                award_data.get("awarded_value"),
                award_data.get("awarded_value_display"),
                award_data.get("awarded_tariff"),
                award_data.get("no_of_bids_received"),
                award_data.get("loa_date"),
                award_data.get("loa_number"),
                award_data.get("agreement_date"),
                award_data.get("loa_pdf_url"),
            )
        )
    except Exception as e:
        print(f"[DB ERROR] Failed to insert award: {e}")
        raise
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# TENDER CONTACTS
# ══════════════════════════════════════════════════════════════

def insert_contact(conn, tender_id: str, contact_data: dict):
    """Insert a contact person record into tender_contacts."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO tender_contacts (
                tender_id, reference_number, contact_type,
                name, designation, department, organization,
                email, phone, mobile, fax,
                address, city, state, pincode, website, source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            """,
            (
                tender_id,
                contact_data.get("reference_number"),
                contact_data.get("contact_type"),
                contact_data.get("name"),
                contact_data.get("designation"),
                contact_data.get("department"),
                contact_data.get("organization"),
                contact_data.get("email"),
                contact_data.get("phone"),
                contact_data.get("mobile"),
                contact_data.get("fax"),
                contact_data.get("address"),
                contact_data.get("city"),
                contact_data.get("state"),
                contact_data.get("pincode"),
                contact_data.get("website"),
                contact_data.get("source", "detail_page"),
            )
        )
    except Exception as e:
        print(f"[DB ERROR] Failed to insert contact: {e}")
        raise
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# SCRAPER RUNS LOG
# ══════════════════════════════════════════════════════════════

def log_scraper_run(conn, portal, batch_id, status,
                    records_found=0, records_new=0, records_updated=0,
                    records_skipped=0, pdfs_downloaded=0, pdfs_parsed=0,
                    error_message=None, stage_reached=None):
    """
    Log a pipeline run to scraper_runs.
    Uses its own commit — independent of the main transaction.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO scraper_runs (
                portal, batch_id, status,
                records_found, records_new, records_updated,
                records_skipped, pdfs_downloaded, pdfs_parsed,
                error_message, stage_reached, finished_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            """,
            (
                portal, batch_id, status,
                records_found, records_new, records_updated,
                records_skipped, pdfs_downloaded, pdfs_parsed,
                error_message, stage_reached,
            ),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] Failed to log scraper run: {e}")
    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# QUICK SELF-TEST
# Run: python core/db.py
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Testing database connection...")
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]

        if tables:
            print(f"✓ Connected to '{DATABASE['dbname']}' on {DATABASE['host']}")
            print(f"✓ Found {len(tables)} tables: {', '.join(tables)}")

            if "tenders" in tables:
                cur.execute("SELECT COUNT(*) FROM tenders")
                count = cur.fetchone()[0]
                print(f"✓ Tenders in database: {count}")

            if "tender_details" in tables:
                cur.execute("SELECT COUNT(*) FROM tender_details")
                count = cur.fetchone()[0]
                print(f"✓ Tender details rows: {count}")
        else:
            print(f"✓ Connected but NO TABLES found.")
            print("  Run: psql -U tender_user -d tender_db -h localhost -f setup_db_v3.sql")

        cur.close()
        conn.close()
        print("\n✓ Database connection test PASSED!")

    except Exception as e:
        print(f"\n✗ Database connection test FAILED: {e}")
        print("\nTroubleshooting:")
        print("  1. Is PostgreSQL running?")
        print("     Mac:     brew services list")
        print("     Windows: services.msc")
        print("  2. Is the password correct in .env?")
        print("  3. Does the database 'tender_db' exist?")
        print("  4. Does the user 'tender_user' exist?")