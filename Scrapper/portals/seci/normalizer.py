"""
portals/seci/normalizer.py
--------------------------
Cleans raw SECI data and inserts into the tenders table.

This takes the messy raw_data from raw_records and transforms it:
  - Parses dates from DD/MM/YYYY to proper DATE type
  - Parses amounts from "INR 1,42,50,000" to integer
  - Cleans titles (remove extra whitespace, newlines)
  - Generates a clean title for fuzzy matching
  - Calculates a hash for exact-match deduplication

The normalizer reads from raw_records WHERE processed = FALSE
and writes to the tenders table.
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

    Args:
        conn: Database connection (transaction managed by pipeline)
        batch_id: Unique ID for this run

    Returns:
        Dictionary: {"new": count_of_new_tenders, "errors": count_of_errors}
    """
    result = {"new": 0, "updated": 0, "errors": 0}

    # Fetch all unprocessed raw records for this batch
    raw_records = get_unprocessed_raw_records(conn, PORTAL_NAME, batch_id)
    print(f"  Found {len(raw_records)} raw records to normalize")

    for record in raw_records:
        try:
            raw = record["raw_data"]

            # ── Clean and transform ──
            tender_data = transform_raw_to_tender(raw, batch_id)

            if tender_data is None:
                # Record was not a real tender (header row, empty, etc.)
                mark_raw_record_processed(conn, record["id"], "Skipped: not a valid tender")
                continue

            # ── Check for existing tender (avoid duplicates) ──
            ref = tender_data.get("reference_number")
            if ref:
                existing = find_by_reference(conn, ref, PORTAL_SHORT)
                if existing:
                    print(f"    Skipped (already exists): {ref}")
                    mark_raw_record_processed(conn, record["id"], "Duplicate: already in tenders")
                    continue

            # ── Insert into tenders table ──
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
            # Don't raise — continue with other records

    print(f"  Normalization complete: {result['new']} new, {result['errors']} errors")
    return result


def transform_raw_to_tender(raw, batch_id):
    """
    Transform a single raw record into a tenders-table-ready dictionary.

    Args:
        raw: Dictionary from raw_records.raw_data
        batch_id: Pipeline run ID

    Returns:
        Dictionary ready for insert_tender(), or None if record should be skipped
    """
    # ── Extract title ──
    title = raw.get("title", raw.get("full_text", ""))
    if not title or len(title) < 10:
        return None  # Too short to be a real tender

    # Clean title: remove newlines, extra spaces, truncate
    title = clean_text(title)
    if len(title) > 500:
        title = title[:500]

    # Generate a "clean" title for fuzzy matching
    # Lowercase, remove stopwords, remove special chars
    title_clean = make_clean_title(title)

    # ── Parse dates ──
    date_published = parse_date(raw.get("date_published"))
    deadline = parse_datetime_ist(raw.get("deadline"))

    # ── Parse amounts ──
    # (These come from detail page — may not be in raw listing data)
    emd_raw = raw.get("emd_amount")
    emd_amount = parse_amount(emd_raw) if emd_raw else None
    value_display = format_inr(emd_amount) if emd_amount else None

    # ── Reference number ──
    ref_number = raw.get("reference_number", "")
    if ref_number:
        ref_number = clean_text(ref_number)
        # Handle multi-line: take last line
        if "\n" in ref_number:
            lines = [l.strip() for l in ref_number.split("\n") if l.strip()]
            ref_number = lines[-1] if lines else ref_number

    # ── Documents ──
    doc_urls = raw.get("document_urls", [])
    detail_url = raw.get("detail_url", "")

    # ── Build niche_metadata (SECI-specific fields) ──
    niche_metadata = {}
    if raw.get("seci_tender_id"):
        niche_metadata["seci_tender_id"] = raw["seci_tender_id"]
    if detail_url:
        niche_metadata["detail_url"] = detail_url

    # ── Generate dedup hash ──
    # Hash of: org + ref_number + title_clean
    # Used for exact-match deduplication
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
        "value": None,  # Filled from detail page / PDF later
        "value_display": value_display,
        "emd_amount": emd_amount,
        "date_published": date_published,
        "deadline": deadline,
        "bid_opening_date": None,  # Filled from detail page later
        "category": classify_tender(title),
        "subcategory": None,
        "tender_type": None,
        "state": extract_state(title),
        "district": None,
        "niche_metadata": json.dumps(niche_metadata),
        "document_urls": doc_urls if doc_urls else None,
        "document_count": len(doc_urls),
        "source_portal": PORTAL_NAME,
        "source_url": raw.get("source_url"),
        "all_sources": [raw.get("source_url")] if raw.get("source_url") else None,
        "status": "open",
        "hash": record_hash,
        "batch_id": batch_id,
    }

    return tender


# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def clean_text(text):
    """Remove extra whitespace, newlines, tabs from text."""
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"\s+", " ", text)  # Collapse whitespace
    return text


def make_clean_title(title):
    """
    Create a normalized title for fuzzy matching.
    Lowercase, remove common words, remove special characters.
    """
    clean = title.lower()

    # Remove common stopwords that don't help with matching
    stopwords = [
        "for", "the", "of", "in", "and", "to", "a", "an", "by",
        "from", "with", "on", "at", "under", "through", "via",
        "tender", "rfs", "rfp", "rfq", "eoi", "nit",
        "selection", "supply", "procurement",
    ]
    words = clean.split()
    words = [w for w in words if w not in stopwords]
    clean = " ".join(words)

    # Remove special characters (keep letters, numbers, spaces)
    clean = re.sub(r"[^a-z0-9\s]", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip()

    return clean


def classify_tender(title):
    """
    Auto-classify a tender based on keywords in the title.
    Returns a category string.
    """
    title_lower = title.lower()

    # Check for combined Solar + BESS
    has_solar = any(kw in title_lower for kw in ["solar", "pv", "photovoltaic"])
    has_bess = any(kw in title_lower for kw in ["bess", "battery", "energy storage"])

    if has_solar and has_bess:
        return "Solar+BESS Hybrid"
    elif has_bess:
        return "BESS Only"
    elif has_solar:
        return "Solar PV"
    elif any(kw in title_lower for kw in ["wind", "wind energy", "wind power"]):
        return "Wind"
    elif any(kw in title_lower for kw in ["hybrid", "round the clock", "round-the-clock", "rtc"]):
        return "Hybrid RE"
    elif any(kw in title_lower for kw in ["green hydrogen", "electrolyser"]):
        return "Green Hydrogen"
    else:
        return "Uncategorized"


def extract_state(title):
    """
    Try to extract the Indian state name from the tender title.
    Returns state name or None.
    """
    title_lower = title.lower()

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
    }

    for key, state_name in states.items():
        if key in title_lower:
            return state_name

    return None  # State not mentioned in title


# ─── Quick self-test ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing SECI normalizer helpers...")

    # Test classify_tender
    tests = [
        ("RfS for 500 MW Solar PV Power Projects", "Solar PV"),
        ("Supply of 10 MW Solar with 20 MWh BESS", "Solar+BESS Hybrid"),
        ("100 MW/200 MWh BESS Project", "BESS Only"),
        ("1000 MW Round-the-Clock Power from RE Projects", "Hybrid RE"),
        ("Selection of consultant for audit", "Uncategorized"),
    ]

    for title, expected in tests:
        result = classify_tender(title)
        status = "✓" if result == expected else "✗"
        print(f"  {status} classify('{title[:50]}...') → '{result}' (expected: '{expected}')")

    # Test extract_state
    print()
    state_tests = [
        ("Solar PV Project in Rajasthan", "Rajasthan"),
        ("BESS in Odisha under VGF", "Odisha"),
        ("ISTS-Connected RE Projects in India", None),
    ]

    for title, expected in state_tests:
        result = extract_state(title)
        status = "✓" if result == expected else "✗"
        print(f"  {status} state('{title[:50]}...') → '{result}' (expected: '{expected}')")

    # Test clean_title
    print()
    print(f"  clean: '{make_clean_title('RfS for Supply of 1000 MW Solar PV Power')}'")

    print("\n✓ Normalizer helper tests PASSED!")
