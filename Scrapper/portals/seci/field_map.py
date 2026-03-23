"""
portals/seci/field_map.py
-------------------------
Maps SECI website table headers to your database column names.

WHY NOT HARDCODE cells[4]?
  Because SECI can rearrange their columns at any time.
  This file reads the header row first, figures out which column
  is where, and then extracts by NAME not by position.

HOW TO UPDATE:
  If SECI adds a new column, just add a new entry to COLUMN_MAP.
  The key = text in the <th> header (lowercase).
  The value = your database field name.
"""


# Map: website header text (lowercase) → database field name
# Use '_skip' to ignore a column, '_special' for custom handling
COLUMN_MAP = {
    "s.no": "_skip",
    "s. no": "_skip",
    "sl.no": "_skip",
    "sl. no": "_skip",
    "#": "_skip",

    "tender id": "seci_tender_id",
    "tenderid": "seci_tender_id",

    "tender ref no": "reference_number",
    "tender ref. no": "reference_number",
    "tender reference": "reference_number",
    "reference no": "reference_number",
    "ref no": "reference_number",

    "tender title": "title",
    "title": "title",
    "tender name": "title",
    "description": "title",

    "publication date": "date_published",
    "published date": "date_published",
    "publish date": "date_published",
    "date of publication": "date_published",

    "bid submission date": "deadline",
    "bid submission": "deadline",
    "closing date": "deadline",
    "last date": "deadline",
    "due date": "deadline",

    "view details": "_detail_link",
    "details": "_detail_link",
    "view": "_detail_link",
}


def build_column_index(header_texts):
    """
    Read the header row and figure out which column index maps to which field.

    Args:
        header_texts: List of strings from <th> elements
                      Example: ["S.No", "Tender ID", "Tender Ref No", ...]

    Returns:
        Dictionary mapping field names to column indices.
        Example: {"seci_tender_id": 1, "reference_number": 2, "title": 4, ...}
    """
    index = {}

    for i, header in enumerate(header_texts):
        if not header:
            continue

        # Clean header text
        clean = header.strip().lower()
        # Remove extra whitespace
        clean = " ".join(clean.split())

        # Try to match against our map
        for map_key, field_name in COLUMN_MAP.items():
            if map_key in clean:
                if field_name != "_skip":
                    index[field_name] = i
                break  # Stop after first match for this header

    return index


def validate_column_index(col_index):
    """
    Check that we found the minimum required columns.
    If critical columns are missing, the website probably changed.

    Args:
        col_index: Dictionary from build_column_index()

    Returns:
        Tuple: (is_valid: bool, missing_fields: list)
    """
    required = ["title"]  # At minimum, we need a title
    recommended = ["reference_number", "date_published", "deadline"]

    missing_required = [f for f in required if f not in col_index]
    missing_recommended = [f for f in recommended if f not in col_index]

    if missing_recommended:
        print(f"  [FIELD MAP WARNING] Missing recommended columns: {missing_recommended}")
        print(f"  Found columns: {col_index}")

    is_valid = len(missing_required) == 0
    return is_valid, missing_required


# ─── Quick self-test ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing SECI field map...")

    # Simulate headers from SECI website
    sample_headers = [
        "S.No",
        "Tender ID",
        "E-Publish Date",
        "Tender Ref No.",
        "Tender Title",
        "Publication Date",
        "Bid Submission Date",
        "View Details",
    ]

    col_idx = build_column_index(sample_headers)
    print(f"  Headers: {sample_headers}")
    print(f"  Mapped: {col_idx}")

    is_valid, missing = validate_column_index(col_idx)
    print(f"  Valid: {is_valid}, Missing: {missing}")

    assert "title" in col_idx, "Title column not found!"
    print("\n✓ Field map test PASSED!")
