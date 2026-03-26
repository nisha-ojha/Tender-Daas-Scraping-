import re
import os
import sys
import logging

logging.getLogger("pdfminer").setLevel(logging.ERROR)

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

# ------------------ MAIN ------------------
def parse_rfs_pdf(pdf_path: str) -> dict:
    text = _extract_text(pdf_path)
    if not text:
        return {"_parse_error": "Could not extract text"}

    result = {}

    try:
        result.update(_extract_capacity(text))
        result.update(_extract_tariff(text))
        result.update(_extract_emd(text))
        result.update(_extract_sd_pg_ld(text))
        result.update(_extract_periods(text))
        result.update(_extract_eligibility(text))
        result.update(_extract_location(text))
        result.update(_extract_bid_system(text))
        result.update(_extract_energy_fields(text))
        result.update(_extract_contract_terms(text))
    except Exception as e:
        print(f"[PARSE ERROR] {e}")

    return {k: v for k, v in result.items() if v is not None}


# ------------------ AMENDMENT AWARE ------------------
def parse_tender_pdf_set(pdf_paths: list[str]) -> dict:
    base_results = {}
    amendment_results = {}

    for path in pdf_paths:
        if not os.path.exists(path):
            continue

        filename = os.path.basename(path).lower()
        parsed = parse_rfs_pdf(path)

        if "_parse_error" in parsed:
            continue

        is_amendment = any(kw in filename for kw in [
            "amendment", "corrigendum", "addendum", "clarification", "erratum"
        ])

        text = _extract_text(path)

        if is_amendment:
            # override logic
            if re.search(r"\bBESS\b.*?(removed|deleted)|storage.*?(removed|deleted)", text, re.IGNORECASE):
                amendment_results["energy_storage_required"] = False

            if re.search(r"peak.*?(removed|deleted)", text, re.IGNORECASE):
                amendment_results.pop("power_type", None)

            amendment_results.update(parsed)
        else:
            base_results.update(parsed)

    return {**base_results, **amendment_results}


# ------------------ TEXT ------------------
def _extract_text(pdf_path: str) -> str:
    try:
        import pdfplumber
        pages_text = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages_text.append(t)
        return "\n".join(pages_text)
    except Exception:
        return ""


# ------------------ CAPACITY ------------------
def _extract_capacity(text: str) -> dict:
    result = {}

    m = re.search(r"(\d{1,5}(?:,\d{3})?(?:\.\d+)?)\s*MW\b", text, re.IGNORECASE)
    if m:
        try:
            result["capacity_mw"] = float(m.group(1).replace(",", ""))
        except:
            pass

    if not re.search(r"\bRTC\b|round[\s-]?the[\s-]?clock", text, re.IGNORECASE):
        m = re.search(r"(\d{1,6})\s*MWh\b", text, re.IGNORECASE)
        if m:
            try:
                result["capacity_mwh"] = float(m.group(1))
            except:
                pass

    return result


# ------------------ FIXED TARIFF ------------------
def _extract_tariff(text: str) -> dict:
    result = {}

    for pat in [
        r"ceiling\s+tariff[^Rs.0-9\n]{0,30}(?:Rs\.?|INR)?\s*([\d]+\.[\d]+)",
        r"tariff\s+not\s+exceeding[^Rs.0-9\n]{0,30}(?:Rs\.?|INR)?\s*([\d]+\.[\d]+)",
        r"maximum\s+tariff\s+of\s+(?:Rs\.?|INR)?\s*([\d]+\.[\d]+)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1))
                if 0.5 < val < 20:
                    result["tariff_ceiling"] = val
                    break
            except:
                pass

    return result


# ------------------ FIXED EMD ------------------
def _extract_emd(text: str) -> dict:
    result = {}

    m = re.search(
        r"(?:EMD|earnest\s+money)[^Rs0-9\n]{0,80}(?:Rs\.?|INR)?\s*([\d,]+)",
        text,
        re.IGNORECASE
    )

    if m:
        raw = m.group(1)
        if raw:
            cleaned = raw.replace(",", "").strip()

            if cleaned.isdigit():
                try:
                    result["emd_amount"] = int(cleaned)
                except:
                    pass

    return result


# ------------------ SD ------------------
def _extract_sd_pg_ld(text: str) -> dict:
    result = {}

    m = re.search(r"([\d.]+)\s*%.*security", text, re.IGNORECASE)
    if m:
        try:
            result["sd_percentage"] = float(m.group(1))
        except:
            pass

    return result


# ------------------ PERIOD ------------------
def _extract_periods(text: str) -> dict:
    result = {}

    m = re.search(r"(\d+)\s*years?.{0,40}PPA", text, re.IGNORECASE)
    if m:
        try:
            val = int(m.group(1))
            if 5 <= val <= 35:
                result["ppa_duration_years"] = val
        except:
            pass

    return result


# ------------------ ELIGIBILITY ------------------
def _extract_eligibility(text: str) -> dict:
    result = {}

    if re.search(r"consortium.*allowed", text, re.IGNORECASE):
        result["consortium_allowed"] = True

    return result


# ------------------ LOCATION ------------------
def _extract_location(text: str) -> dict:
    result = {}

    m = re.search(r"state\s+of\s+([A-Z][a-z]+)", text)
    if m:
        result["state"] = m.group(1)

    return result


# ------------------ BID SYSTEM ------------------
def _extract_bid_system(text: str) -> dict:
    result = {}

    if re.search(r"two\s+(?:bid|envelope|cover|stage)", text, re.IGNORECASE):
        result["no_of_covers"] = 2
        result["bid_system_type"] = "Two-Bid"

    return result


# ------------------ ENERGY ------------------
def _extract_energy_fields(text: str) -> dict:
    result = {}

    # Power type hierarchy
    if re.search(r"\bRTC\b|round[\s-]?the[\s-]?clock", text, re.IGNORECASE):
        result["power_type"] = "RTC"

    elif re.search(r"assured\s+peak\s+power\s+supply", text, re.IGNORECASE):
        result["power_type"] = "Peak"

    elif re.search(r"solar\s+pv|solar\s+power|photovoltaic|grid.connected\s+solar", text, re.IGNORECASE):
        result["power_type"] = "Solar"

    elif re.search(r"\bwind\b", text, re.IGNORECASE):
        result["power_type"] = "Wind"

    # Connectivity FIX
    if re.search(r"\bSTU\b|state\s+transmission|STU\s+network", text, re.IGNORECASE):
        result["connectivity"] = "STU"

    elif re.search(r"\bISTS\b|inter.?state\s+transmission", text, re.IGNORECASE):
        result["connectivity"] = "ISTS"

    if re.search(r"reverse\s+auction", text, re.IGNORECASE):
        result["reverse_auction"] = True

    return result


# ------------------ CONTRACT ------------------
def _extract_contract_terms(text: str) -> dict:
    result = {}

    if re.search(r"\bDCR\b", text):
        result["domestic_content_req"] = True

    return result


# ------------------ DB UPDATE (FIXED) ------------------
def apply_rfs_data_to_tender(conn, tender_id, data):
    import json

    field_map = {
        "capacity_mw": "capacity_mw",
        "capacity_mwh": "capacity_mwh",
        "tariff_ceiling": "tariff_ceiling",
        "emd_amount": "emd_amount",
        "sd_percentage": "sd_percentage",
        "pg_percentage": "pg_percentage",
        "ppa_duration_years": "ppa_duration_years",
        "consortium_allowed": "consortium_allowed",
        "state": "state",
        "no_of_covers": "no_of_covers",
        "bid_system_type": "bid_system_type",
        "power_type": "power_type",
        "energy_storage_required": "energy_storage_required",
        "connectivity": "connectivity",
        "reverse_auction": "reverse_auction",
    }

    updates = {col: data[src] for src, col in field_map.items() if src in data}

    if "eligibility" in data:
        updates["eligibility"] = json.dumps(data["eligibility"])

    if not updates:
        print(f"    No fields to update for tender {tender_id}")
        return

    set_clause = ", ".join(f"{col} = %s" for col in updates)
    values = list(updates.values()) + [tender_id]

    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE tenders SET {set_clause}, pdfs_parsed = TRUE WHERE id = %s",
            values
        )
        print(f"    Updated {len(updates)} fields for tender {tender_id}")
    finally:
        cur.close()