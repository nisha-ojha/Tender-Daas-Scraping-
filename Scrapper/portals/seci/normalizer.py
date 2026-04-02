"""
portals/seci/normalizer.py
--------------------------
Cleans raw SECI data and inserts into the v3 schema tables:
  tenders, tender_details, tender_financial, tender_technical

CHANGE DETECTION:
  On re-scrape, compares key fields against DB.
  If changed → apply_updates() updates the correct table
             → logs change to tender_changes for audit trail
  If unchanged → skips silently (no DB write)
"""

import re
import hashlib
import psycopg2.extras

from core.db import (
    get_unprocessed_raw_records,
    mark_raw_record_processed,
    insert_tender,
    find_by_reference,
)
from core.date_parser import parse_date, parse_datetime_ist
from core.value_parser import parse_amount, format_inr
from portals.seci.config import PORTAL_NAME, PORTAL_SHORT, PORTAL_FULL_NAME


# ══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

def normalize(conn, batch_id):
    """
    Main entry point — called by pipeline.py.
    Processes all unprocessed raw records for this batch.
    """
    result = {"new": 0, "updated": 0, "errors": 0}

    raw_records = get_unprocessed_raw_records(conn, PORTAL_NAME, batch_id)
    print(f"  Found {len(raw_records)} raw records to normalize")

    cur = conn.cursor()

    for record in raw_records:
        # Use savepoint so one failure doesn't abort the whole transaction
        cur.execute("SAVEPOINT norm_record")
        try:
            raw = record["raw_data"]
            tender_data = transform_raw_to_tender(raw, batch_id)

            if tender_data is None:
                mark_raw_record_processed(conn, record["id"], "Skipped: not a valid tender")
                cur.execute("RELEASE SAVEPOINT norm_record")
                continue

            ref = tender_data.get("reference_number")
            if ref:
                existing = find_by_reference(conn, ref, PORTAL_NAME)

                if existing:
                    # ── Change detection ──────────────────────────
                    changes = detect_changes(conn, existing["id"], tender_data)

                    if changes:
                        apply_updates(conn, existing["id"], changes, batch_id)
                        result["updated"] += 1
                        print(f"    Updated ({len(changes)} fields): {ref}")
                    else:
                        print(f"    No change: {ref}")

                    mark_raw_record_processed(conn, record["id"], "Checked: existing tender")
                    cur.execute("RELEASE SAVEPOINT norm_record")
                    continue

            # ── New tender ────────────────────────────────────────
            tender_id = insert_tender(conn, tender_data)

            if tender_id:
                result["new"] += 1
                mark_raw_record_processed(conn, record["id"])
            else:
                mark_raw_record_processed(conn, record["id"], "Insert returned None (conflict?)")

            cur.execute("RELEASE SAVEPOINT norm_record")

        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT norm_record")
            cur.execute("RELEASE SAVEPOINT norm_record")
            result["errors"] += 1
            err_msg = str(e).split("\n")[0]
            print(f"    [NORMALIZE ERROR] Record {record['id']}: {err_msg}")
            try:
                mark_raw_record_processed(conn, record["id"], f"Error: {err_msg[:200]}")
            except Exception:
                pass

    cur.close()
    print(
        f"  Normalization complete: "
        f"{result['new']} new, "
        f"{result['updated']} updated, "
        f"{result['errors']} errors"
    )
    return result


# ══════════════════════════════════════════════════════════════
# TRANSFORM — maps raw scraped data to v3 schema dict
# ══════════════════════════════════════════════════════════════

def transform_raw_to_tender(raw, batch_id):
    """
    Map raw SECI scraped data → v3 schema dict.
    Returned dict is consumed by db.insert_tender() which writes to:
      tenders, tender_details, tender_financial, tender_technical
    """
    # ── Title ─────────────────────────────────────────────────
    title = raw.get("title", raw.get("full_text", ""))
    if not title or len(title) < 10:
        return None

    title = clean_text(title)[:500]
    title_clean = make_clean_title(title)

    detail = raw.get("detail", {}) or {}

    # ── Reference number ──────────────────────────────────────
    ref_number = clean_text(raw.get("reference_number", ""))
    if "\n" in ref_number:
        lines = [l.strip() for l in ref_number.split("\n") if l.strip()]
        ref_number = lines[-1] if lines else ref_number
    ref_number = ref_number or None

    # ── Status ────────────────────────────────────────────────
    tender_status = raw.get("tender_status", "live")
    status_map = {"live": "open", "archive": "closed", "result": "awarded"}
    db_status = status_map.get(tender_status, "open")

    # ── Dates ─────────────────────────────────────────────────
    date_published = parse_date(
        get_detail_value(detail, "Tender Publication Date")
        or raw.get("date_published")
    )

    pre_bid_date = parse_datetime_ist(
        get_detail_value(detail, "Pre Bid Meeting Date")
    )

    bid_submission_online = parse_datetime_ist(
        get_detail_value(detail, "Bid Submission End Date (Online)")
        or raw.get("deadline")
    )

    bid_submission_offline = parse_datetime_ist(
        get_detail_value(detail, "Bid Submission End Date (Offline)")
    )

    deadline = bid_submission_online

    bid_opening_date = parse_datetime_ist(
        get_detail_value(detail, "Bid Open Date")
    )

    # ── Financial ─────────────────────────────────────────────
    emd_raw = (
        get_detail_value(detail, "EMD")
        or get_detail_value(detail, "EMD Amount")
        or raw.get("emd_amount")
    )
    emd_amount = parse_amount(emd_raw) if emd_raw else None

    fee_raw = (
        get_detail_value(detail, "Tender Fee")
        or get_detail_value(detail, "Tender Fee/Bid Processing Fee")
    )
    tender_fee = parse_amount(fee_raw) if fee_raw else None

    # ── CPPP cross-reference ──────────────────────────────────
    cppp_id = clean_text(
        get_detail_value(detail, "Tender ID On CPPP")
        or get_detail_value(detail, "CPPP Tender ID")
        or ""
    ) or None

    # ── Description ───────────────────────────────────────────
    description = get_detail_value(detail, "Tender Description")
    if description:
        description = clean_text(description)[:2000]

    # ── Documents ─────────────────────────────────────────────
    doc_list = detail.get("documents", [])
    announcement_list = detail.get("announcements", [])
    corrigendum_count = len(announcement_list)

    extra_data = {}
    if doc_list:
        extra_data["documents"] = doc_list
    if announcement_list:
        extra_data["announcements"] = announcement_list

    # ── Hash ──────────────────────────────────────────────────
    hash_input = f"{PORTAL_NAME}|{ref_number}|{title_clean}"
    record_hash = hashlib.md5(hash_input.encode()).hexdigest()

    # ── Build v3 schema dict ───────────────────────────────────
    return {
        # ── tenders ───────────────────────────────────────────
        "portal":              PORTAL_NAME,
        "reference_number":    ref_number,
        "cppp_tender_id":      cppp_id,
        "title":               title,
        "title_clean":         title_clean,
        "description":         description,
        "organization":        PORTAL_FULL_NAME,
        "organization_short":  PORTAL_SHORT,
        "department":          None,
        "ministry":            None,
        "category":            classify_tender(title, description),
        "subcategory":         None,
        "tender_type":         clean_text(get_detail_value(detail, "Tender Type") or ""),
        "procurement_type":    None,
        "source_url":          raw.get("source_url"),
        "detail_url":          raw.get("detail_url"),
        "all_sources":         [raw.get("source_url")] if raw.get("source_url") else None,
        "batch_id":            batch_id,
        "hash":                record_hash,

        # ── tender_details ────────────────────────────────────
        "status":                  db_status,
        "date_published":          date_published,
        "pre_bid_date":            pre_bid_date,
        "bid_submission_online":   bid_submission_online,
        "bid_submission_offline":  bid_submission_offline,
        "deadline":                deadline,
        "bid_opening_date":        bid_opening_date,
        "financial_bid_opening":   None,
        "notification_number":     None,
        "corrigendum_count":       corrigendum_count,
        "state":                   extract_state(title, description),
        "district":                None,
        "region":                  None,
        "location_text":           None,
        "extra_data":              extra_data,

        # ── tender_financial ──────────────────────────────────
        "estimated_value":         None,
        "estimated_value_display": format_inr(emd_amount) if emd_amount else None,
        "value_is_estimated":      True,
        "emd_amount":              emd_amount,
        "emd_raw_text":            emd_raw,
        "emd_per_mw":              None,
        "emd_currency":            "INR",
        "emd_is_formula":          False,
        "emd_exemption_msme":      False,
        "emd_exemption_startup":   False,
        "tender_fee":              tender_fee,
        "tender_fee_raw":          fee_raw,
        "tender_fee_refundable":   False,
        "processing_fee":          None,
        "processing_fee_per_mw":   None,
        "processing_fee_raw":      None,
        "sd_percentage":           None,
        "sd_raw_text":             None,
        "sd_form":                 None,
        "pg_percentage":           None,
        "pg_raw_text":             None,
        "pg_validity_months":      None,
        "pg_form":                 None,
        "liquidated_damages_pct":  None,
        "ld_cap_pct":              None,
        "tariff_ceiling":          None,
        "tariff_floor":            None,
        "l1_tariff":               None,
        "tariff_type":             None,
        "payment_security":        None,
        "mobilisation_advance":    False,
        "advance_percentage":      None,

        # ── tender_technical ──────────────────────────────────
        "capacity_mw":             None,
        "capacity_mwh":            None,
        "capacity_kw":             None,
        "capacity_raw_text":       None,
        "power_type":              None,
        "energy_storage_required": None,
        "project_model":           None,
        "connectivity":            None,
        "interconnection_point":   None,
        "substation_kv":           None,
        "land_acres":              None,
        "land_responsibility":     None,
        "no_of_covers":            None,
        "bid_system_type":         None,
        "reverse_auction":         False,
        "domestic_content_req":    None,
        "is_international":        False,
        "contract_type":           None,
        "om_period_years":         None,
        "ppa_duration_years":      None,
        "concession_period_years": None,
        "net_worth_required_cr":   None,
        "turnover_required_cr":    None,
        "experience_required_mw":  None,
        "experience_raw_text":     None,
        "consortium_allowed":      None,
        "max_consortium_members":  None,
        "foreign_bidder_allowed":  None,
        "msme_exemption":          None,
        "startup_eligible":        None,
        "eligibility":             None,
    }


# ══════════════════════════════════════════════════════════════
# CHANGE DETECTION
# ══════════════════════════════════════════════════════════════

def detect_changes(conn, tender_id: str, new_data: dict) -> dict:
    """
    Compare newly scraped data against what is stored in the DB.

    Only checks fields that SECI updates regularly:
      - status (open → closed → awarded)
      - deadline (can be extended)
      - corrigendum_count (amendments added)
      - emd_amount (rarely changes)
      - tender_fee (rarely changes)
      - pre_bid_date (can change)
      - bid_opening_date (can be postponed)

    Returns:
        dict of {field: {"old": old_val, "new": new_val}}
        Empty dict if nothing changed.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT
                d.status,
                d.deadline,
                d.corrigendum_count,
                d.bid_opening_date,
                d.pre_bid_date,
                f.emd_amount,
                f.tender_fee
            FROM tenders t
            JOIN tender_details d   ON d.tender_id = t.id
            JOIN tender_financial f ON f.tender_id = t.id
            WHERE t.id = %s
        """, (tender_id,))

        existing = cur.fetchone()
        if not existing:
            return {}

        # Fields to watch — (db_value, new_scraped_value)
        watch = {
            "status":            (existing["status"],            new_data.get("status")),
            "deadline":          (existing["deadline"],          new_data.get("deadline")),
            "corrigendum_count": (existing["corrigendum_count"], new_data.get("corrigendum_count")),
            "emd_amount":        (existing["emd_amount"],        new_data.get("emd_amount")),
            "tender_fee":        (existing["tender_fee"],        new_data.get("tender_fee")),
            "pre_bid_date":      (existing["pre_bid_date"],      new_data.get("pre_bid_date")),
            "bid_opening_date":  (existing["bid_opening_date"],  new_data.get("bid_opening_date")),
        }

        changes = {}
        for field, (old_val, new_val) in watch.items():
            # Skip if new value not scraped
            if new_val is None:
                continue
            # Compare as strings to handle datetime/decimal differences
            if str(old_val) != str(new_val):
                changes[field] = {"old": str(old_val), "new": str(new_val)}

        return changes

    finally:
        cur.close()


def apply_updates(conn, tender_id: str, changes: dict, batch_id: str):
    """
    Apply detected changes to the correct tables.
    Also logs each change to tender_changes for audit trail.

    Tables updated:
      tender_details  → status, deadline, corrigendum_count,
                        pre_bid_date, bid_opening_date
      tender_financial → emd_amount, tender_fee
      tender_changes  → full audit log of every change
    """
    cur = conn.cursor()
    try:
        # ── Update tender_details ─────────────────────────────
        detail_fields = [
            "status", "deadline", "corrigendum_count",
            "pre_bid_date", "bid_opening_date"
        ]
        detail_updates = {
            f: changes[f]["new"]
            for f in detail_fields
            if f in changes
        }

        if detail_updates:
            set_clause = ", ".join(f"{k} = %s" for k in detail_updates)
            cur.execute(
                f"""
                UPDATE tender_details
                SET {set_clause}, updated_at = NOW()
                WHERE tender_id = %s
                """,
                list(detail_updates.values()) + [tender_id]
            )

        # ── Update tender_financial ───────────────────────────
        financial_fields = ["emd_amount", "tender_fee"]
        financial_updates = {
            f: changes[f]["new"]
            for f in financial_fields
            if f in changes
        }

        if financial_updates:
            set_clause = ", ".join(f"{k} = %s" for k in financial_updates)
            cur.execute(
                f"""
                UPDATE tender_financial
                SET {set_clause}, updated_at = NOW()
                WHERE tender_id = %s
                """,
                list(financial_updates.values()) + [tender_id]
            )

        # ── Log every change to tender_changes ────────────────
        for field, vals in changes.items():
            change_type = (
                "status_changed"    if field == "status"            else
                "deadline_extended" if field == "deadline"          else
                "corrigendum_added" if field == "corrigendum_count" else
                "prebid_changed"    if field == "pre_bid_date"      else
                "opening_postponed" if field == "bid_opening_date"  else
                "value_updated"
            )

            cur.execute(
                """
                INSERT INTO tender_changes
                    (tender_id, change_type, field_name, old_value, new_value)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (tender_id, change_type, field, vals["old"], vals["new"])
            )

    finally:
        cur.close()


# ══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════

def get_detail_value(detail, label):
    """
    Get a value from the detail dict by label.
    Handles SECI's quirk where label and value can be swapped
    in their 2-column HTML tables.
    """
    if not detail or not label:
        return None

    # Normal lookup: detail["EMD"] → "Rs. 59,000"
    value = detail.get(label)
    if value and value not in ("documents", "announcements", "all_links", "_error"):
        if not isinstance(value, (list, dict)):
            return value

    # Swapped lookup: find key whose value equals the label
    for key, val in detail.items():
        if isinstance(val, str) and val == label:
            return key

    return None


def clean_text(text):
    """Remove extra whitespace and normalize."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


def make_clean_title(title):
    """
    Normalize title for fuzzy dedup matching.
    Lowercased, stopwords removed, punctuation stripped.
    """
    clean = title.lower()
    stopwords = {
        "for", "the", "of", "in", "and", "to", "a", "an", "by",
        "from", "with", "on", "at", "under", "through", "via",
        "tender", "rfs", "rfp", "rfq", "eoi", "nit",
        "selection", "supply", "procurement",
    }
    words = [w for w in clean.split() if w not in stopwords]
    clean = " ".join(words)
    clean = re.sub(r"[^a-z0-9\s]", "", clean)
    return re.sub(r"\s+", " ", clean).strip()


def classify_tender(title, description=None):
    """Auto-classify tender based on keywords in title and description."""
    text = (title or "").lower()
    if description:
        text += " " + description.lower()

    has_solar = any(kw in text for kw in ["solar", "pv", "photovoltaic"])
    has_bess  = any(kw in text for kw in ["bess", "battery", "energy storage"])
    has_wind  = any(kw in text for kw in ["wind energy", "wind power", "wind farm"])
    has_rtc   = any(kw in text for kw in ["round the clock", "rtc"])
    has_h2    = any(kw in text for kw in ["green hydrogen", "electrolyser"])

    if has_solar and has_bess:   return "Solar+BESS Hybrid"
    if has_bess:                 return "BESS Only"
    if has_solar:                return "Solar PV"
    if has_wind:                 return "Wind"
    if has_rtc:                  return "Hybrid RE"
    if has_h2:                   return "Green Hydrogen"

    # Equipment and services
    if any(kw in text for kw in ["module", "inverter", "transformer", "cable"]):
        return "Equipment Supply"
    if any(kw in text for kw in ["o&m", "operation", "maintenance", "amc"]):
        return "O&M"
    if any(kw in text for kw in ["consultancy", "consulting", "agency", "manpower"]):
        return "Consultancy"
    if any(kw in text for kw in ["epc", "construction", "civil"]):
        return "EPC"
    if any(kw in text for kw in ["it", "software", "portal", "system", "cyber"]):
        return "IT"

    return "Uncategorized"


def extract_state(title, description=None):
    """Extract Indian state name from title or description."""
    text = (title or "").lower()
    if description:
        text += " " + description.lower()

    states = {
        "rajasthan":       "Rajasthan",
        "gujarat":         "Gujarat",
        "tamil nadu":      "Tamil Nadu",
        "karnataka":       "Karnataka",
        "andhra pradesh":  "Andhra Pradesh",
        "telangana":       "Telangana",
        "maharashtra":     "Maharashtra",
        "madhya pradesh":  "Madhya Pradesh",
        "uttar pradesh":   "Uttar Pradesh",
        "odisha":          "Odisha",
        "jharkhand":       "Jharkhand",
        "kerala":          "Kerala",
        "west bengal":     "West Bengal",
        "chhattisgarh":    "Chhattisgarh",
        "haryana":         "Haryana",
        "punjab":          "Punjab",
        "assam":           "Assam",
        "bihar":           "Bihar",
        "ladakh":          "Ladakh",
        "himachal pradesh":"Himachal Pradesh",
        "uttarakhand":     "Uttarakhand",
        "goa":             "Goa",
        "tripura":         "Tripura",
        "meghalaya":       "Meghalaya",
        "manipur":         "Manipur",
    }

    for key, state_name in states.items():
        if key in text:
            return state_name

    return None