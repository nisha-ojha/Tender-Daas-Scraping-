"""
portals/seci/normalizer.py
--------------------------
Cleans raw SECI data and inserts into the tenders table.

HANDLES:
  - Listing page data (title, ref, dates from table)
  - Detail page data (EMD, bid dates, CPPP ID, documents)
  - Tender status mapping (live→open, archive→closed, result→awarded)
"""

import re
import json
import hashlib
from datetime import datetime

from core.db import (
    get_unprocessed_raw_records,
    mark_raw_record_processed,
    insert_tender,
    find_by_reference,
)
from core.date_parser import parse_date, parse_datetime_ist
from core.value_parser import parse_amount, format_inr
from portals.seci.config import PORTAL_NAME, PORTAL_SHORT, PORTAL_FULL_NAME


def normalize(conn, batch_id):
    """
    Main entry point — called by pipeline.py
    """
    result = {"new": 0, "updated": 0, "errors": 0}

    raw_records = get_unprocessed_raw_records(conn, PORTAL_NAME, batch_id)
    print(f"  Found {len(raw_records)} raw records to normalize")

    for record in raw_records:
        try:
            raw = record["raw_data"]
            tender_data = transform_raw_to_tender(raw, batch_id)

            if tender_data is None:
                mark_raw_record_processed(conn, record["id"], "Skipped: not a valid tender")
                continue

            # Check for existing tender
            ref = tender_data.get("reference_number")
            if ref:
                existing = find_by_reference(conn, ref, PORTAL_SHORT)
                if existing:
                    print(f"    Skipped (already exists): {ref}")
                    mark_raw_record_processed(conn, record["id"], "Duplicate: already in tenders")
                    continue

            tender_id = insert_tender(conn, tender_data)

            if tender_id:
                result["new"] += 1
                mark_raw_record_processed(conn, record["id"])
            else:
                mark_raw_record_processed(conn, record["id"], "Insert returned None (conflict?)")

        except Exception as e:
            result["errors"] += 1
            print(f"    [NORMALIZE ERROR] Record {record['id']}: {e}")
            mark_raw_record_processed(conn, record["id"], f"Error: {str(e)[:200]}")

    print(f"  Normalization complete: {result['new']} new, {result['errors']} errors")
    return result


def transform_raw_to_tender(raw, batch_id):
    """
    Transform a single raw record into a tenders-table-ready dictionary.
    Reads both listing data and detail page data (flat dict).
    """
    # ── Extract title ──
    title = raw.get("title", raw.get("full_text", ""))
    if not title or len(title) < 10:
        return None

    title = clean_text(title)
    if len(title) > 500:
        title = title[:500]

    title_clean = make_clean_title(title)

    # ── Get detail page data ──
    # The scraper stores detail fields as a FLAT dict:
    #   detail["Tender Publication Date"] = "10/03/2026 15:50:31"
    #   detail["EMD"] = "INR 1,42,50,000"
    #   detail["Tender ID On CPPP"] = "2026_SECI_829629_1"
    detail = raw.get("detail", {})

    # ── Parse dates from LISTING ──
    date_published = parse_date(raw.get("date_published"))
    deadline = parse_datetime_ist(raw.get("deadline"))
    bid_opening_date = None

    # ── Override dates from DETAIL page (more accurate, has exact time) ──
    if detail:
        pub_detail = detail.get("Tender Publication Date")
        if pub_detail:
            date_published = parse_date(pub_detail) or date_published

        deadline_detail = detail.get("Bid Submission End Date (Online)")
        if deadline_detail:
            deadline = parse_datetime_ist(deadline_detail) or deadline

        bid_open = detail.get("Bid Open Date")
        if bid_open:
            bid_opening_date = parse_date(bid_open)

    # ── Parse EMD amount from detail page ──
    emd_raw = detail.get("EMD") or detail.get("EMD Amount") or raw.get("emd_amount")
    emd_amount = parse_amount(emd_raw) if emd_raw else None
    value_display = format_inr(emd_amount) if emd_amount else None

    # ── Reference number ──
    ref_number = raw.get("reference_number", "")
    if ref_number:
        ref_number = clean_text(ref_number)
        if "\n" in ref_number:
            lines = [l.strip() for l in ref_number.split("\n") if l.strip()]
            ref_number = lines[-1] if lines else ref_number

    # ── Tender status (live / archive / result) ──
    tender_status = raw.get("tender_status", "live")
    status_map = {
        "live": "open",
        "archive": "closed",
        "result": "awarded",
    }
    db_status = status_map.get(tender_status, "open")

    # ── Documents ──
    doc_urls = raw.get("document_urls", [])
    detail_url = raw.get("detail_url", "")

    # ── Build niche_metadata (SECI-specific fields) ──
    niche_metadata = {}

    if raw.get("seci_tender_id"):
        niche_metadata["seci_tender_id"] = raw["seci_tender_id"]
    if detail_url:
        niche_metadata["detail_url"] = detail_url
    if tender_status:
        niche_metadata["tender_status_original"] = tender_status

    # CPPP Tender ID (for cross-portal deduplication)
    cppp_id = detail.get("Tender ID On CPPP") or detail.get("CPPP Tender ID")
    if cppp_id:
        niche_metadata["cppp_tender_id"] = clean_text(cppp_id)

    # Full description from detail page
    full_desc = detail.get("Tender Description")
    if full_desc:
        niche_metadata["full_description"] = full_desc[:2000]

    # Pre-bid meeting date
    prebid = detail.get("Pre Bid Meeting Date")
    if prebid:
        niche_metadata["pre_bid_date"] = prebid

    # Tender fee
    fee_raw = detail.get("Tender Fee") or detail.get("Tender Fee/Bid Processing Fee")
    if fee_raw:
        fee_amount = parse_amount(fee_raw)
        if fee_amount is not None:
            niche_metadata["tender_fee"] = fee_amount
        niche_metadata["tender_fee_raw"] = fee_raw

    # Offline submission date
    offline_date = detail.get("Bid Submission End Date (Offline)")
    if offline_date:
        niche_metadata["bid_submission_offline"] = offline_date

    # Store ALL detail page fields (so nothing is lost)
    if detail and "_error" not in detail:
        niche_metadata["detail_page_fields"] = detail

    # ── Generate dedup hash ──
    hash_input = f"{PORTAL_SHORT}|{ref_number}|{title_clean}"
    record_hash = hashlib.md5(hash_input.encode()).hexdigest()

    # ── Build final tender record ──
    tender = {
        "reference_number": ref_number or None,
        "title": title,
        "title_clean": title_clean,
        "organization": PORTAL_FULL_NAME,
        "organization_short": PORTAL_SHORT,
        "department": None,
        "value": None,
        "value_display": value_display,
        "emd_amount": emd_amount,
        "date_published": date_published,
        "deadline": deadline,
        "bid_opening_date": bid_opening_date,
        "category": classify_tender(title, full_desc),
        "subcategory": None,
        "tender_type": detail.get("Tender Type"),
        "state": extract_state(title, full_desc),
        "district": None,
        "niche_metadata": json.dumps(niche_metadata),
        "document_urls": doc_urls if doc_urls else None,
        "document_count": len(doc_urls),
        "source_portal": PORTAL_NAME,
        "source_url": raw.get("source_url"),
        "all_sources": [raw.get("source_url")] if raw.get("source_url") else None,
        "status": db_status,
        "hash": record_hash,
        "batch_id": batch_id,
    }

    return tender


# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def clean_text(text):
    """Remove extra whitespace, newlines, tabs."""
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text


def make_clean_title(title):
    """Create a normalized title for fuzzy matching."""
    clean = title.lower()
    stopwords = [
        "for", "the", "of", "in", "and", "to", "a", "an", "by",
        "from", "with", "on", "at", "under", "through", "via",
        "tender", "rfs", "rfp", "rfq", "eoi", "nit",
        "selection", "supply", "procurement",
    ]
    words = clean.split()
    words = [w for w in words if w not in stopwords]
    clean = " ".join(words)
    clean = re.sub(r"[^a-z0-9\s]", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def classify_tender(title, description=None):
    """Auto-classify based on keywords in title AND description."""
    text = title.lower()
    if description:
        text += " " + description.lower()

    has_solar = any(kw in text for kw in ["solar", "pv", "photovoltaic"])
    has_bess = any(kw in text for kw in ["bess", "battery", "energy storage"])

    if has_solar and has_bess:
        return "Solar+BESS Hybrid"
    elif has_bess:
        return "BESS Only"
    elif has_solar:
        return "Solar PV"
    elif any(kw in text for kw in ["wind energy", "wind power", "wind farm"]):
        return "Wind"
    elif any(kw in text for kw in ["hybrid", "round the clock", "round-the-clock", "rtc"]):
        return "Hybrid RE"
    elif any(kw in text for kw in ["green hydrogen", "electrolyser"]):
        return "Green Hydrogen"
    else:
        return "Uncategorized"


def extract_state(title, description=None):
    """Extract Indian state name from title or description."""
    text = title.lower()
    if description:
        text += " " + description.lower()

    states = {
        "rajasthan": "Rajasthan",
        "gujarat": "Gujarat",
        "tamil nadu": "Tamil Nadu",
        "karnataka": "Karnataka",
        "andhra pradesh": "Andhra Pradesh",
        "telangana": "Telangana",
        "maharashtra": "Maharashtra",
        "madhya pradesh": "Madhya Pradesh",
        "uttar pradesh": "Uttar Pradesh",
        "odisha": "Odisha",
        "jharkhand": "Jharkhand",
        "kerala": "Kerala",
        "west bengal": "West Bengal",
        "chhattisgarh": "Chhattisgarh",
        "haryana": "Haryana",
        "punjab": "Punjab",
        "ladakh": "Ladakh",
        "lakshadweep": "Lakshadweep",
        "assam": "Assam",
        "bihar": "Bihar",
        "goa": "Goa",
        "himachal pradesh": "Himachal Pradesh",
        "uttarakhand": "Uttarakhand",
        "tripura": "Tripura",
        "meghalaya": "Meghalaya",
        "manipur": "Manipur",
        "mizoram": "Mizoram",
        "nagaland": "Nagaland",
        "arunachal pradesh": "Arunachal Pradesh",
        "sikkim": "Sikkim",
    }

    for key, state_name in states.items():
        if key in text:
            return state_name

    return None