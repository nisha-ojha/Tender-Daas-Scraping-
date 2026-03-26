"""
core/pdf/classifier.py
----------------------
Classifies a PDF into one of these types:
  RfS       — Request for Selection (main tender document)
  RfP       — Request for Proposal
  NIT       — Notice Inviting Tender
  BOQ       — Bill of Quantities
  BidOpening — Bid Opening Statement / Comparative Statement
  LoA       — Letter of Award / Work Order
  Corrigendum — Amendment / Addendum
  PPA       — Power Purchase Agreement
  PreBid    — Pre-Bid Meeting Minutes / Queries
  Drawing   — Technical Drawing / Layout
  Other     — Anything else
"""

import re


# ── Keyword maps (filename-based) ────────────────────────────
NAME_RULES = [
    ("BidOpening",  ["bid opening", "comparative statement", "bos", "bid open", "financial bid"]),
    ("LoA",         ["letter of award", "loa", "work order", "award letter", "letter_of_award"]),
    ("BOQ",         ["boq", "bill of quantity", "bill of quantities", "schedule of rates", "sor"]),
    ("Corrigendum", ["corrigendum", "addendum", "amendment", "erratum", "rectification"]),
    ("PPA",         ["ppa", "power purchase agreement", "psa", "power sale agreement"]),
    ("PreBid",      ["pre-bid", "prebid", "pre bid", "meeting minutes", "queries", "clarification"]),
    ("Drawing",     ["drawing", "layout", "diagram", "map", "drg"]),
    ("NIT",         ["nit", "notice inviting", "notice_inviting"]),
    ("RfS",         ["rfs", "rfs_", "_rfs", "request for selection", "selection document"]),
    ("RfP",         ["rfp", "request for proposal", "rfq", "request for quotation"]),
]

# ── Keyword maps (first-page content-based) ──────────────────
CONTENT_RULES = [
    ("BidOpening",  ["bid opening statement", "comparative statement of bids",
                     "financial bid opening", "price bid opening"]),
    ("LoA",         ["letter of award", "this is to inform", "pleased to award",
                     "awarded to m/s", "work order no"]),
    ("BOQ",         ["bill of quantities", "bill of quantity", "schedule of rates",
                     "item description", "unit rate", "quantity"]),
    ("Corrigendum", ["corrigendum", "this corrigendum", "amendment to tender"]),
    ("PPA",         ["power purchase agreement", "this agreement is entered",
                     "seller agrees to sell"]),
    ("PreBid",      ["pre-bid meeting", "queries raised", "clarifications sought"]),
    ("RfS",         ["request for selection", "selection of solar", "selection of wind",
                     "rfs document", "rfp document", "request for proposal"]),
]


def classify_by_name(doc_name: str) -> str:
    """
    Classify PDF type from the document name/filename.
    Fast — no file reading required.

    Args:
        doc_name: Document name from the tender detail page
                  e.g. "RfS_for_1200MW_Solar.pdf"

    Returns:
        Document type string: "RfS", "BOQ", "BidOpening", etc.
    """
    if not doc_name:
        return "Other"

    name_lower = doc_name.lower().replace("_", " ").replace("-", " ")

    for doc_type, keywords in NAME_RULES:
        if any(kw in name_lower for kw in keywords):
            return doc_type

    # Generic tender document
        if any(kw in name_lower for kw in ["tender", "rfp", "rfs", "nit"]):
            return "RfS"

    return "Other"


def classify_by_content(first_page_text: str, doc_name: str = "") -> str:
    """
    Classify PDF type using first-page text content.
    More accurate than name-based, but requires reading the file.

    Args:
        first_page_text: Text extracted from first page of PDF
        doc_name:        Optional filename for tie-breaking

    Returns:
        Document type string
    """
    if not first_page_text:
        return classify_by_name(doc_name)

    text_lower = first_page_text.lower()

    for doc_type, keywords in CONTENT_RULES:
        if any(kw in text_lower for kw in keywords):
            return doc_type

    # Fall back to name-based
    return classify_by_name(doc_name)


def classify_pdf_file(pdf_path: str, doc_name: str = "") -> str:
    """
    Classify a PDF by reading its first page.
    Use this when you have the file on disk.

    Args:
        pdf_path: Full path to the PDF file
        doc_name: Original document name (used as fallback)

    Returns:
        Document type string
    """
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return classify_by_name(doc_name)
            first_page_text = pdf.pages[0].extract_text() or ""

        return classify_by_content(first_page_text, doc_name)

    except Exception:
        # If PDF can't be read, fall back to name
        return classify_by_name(doc_name)


def is_parseable_type(doc_type: str) -> bool:
    """
    Returns True if we have a parser for this document type.
    Used by the PDF pipeline to decide whether to attempt parsing.
    """
    return doc_type in {"RfS", "RfP", "NIT", "BOQ", "BidOpening", "LoA"}


# ── Standalone test ───────────────────────────────────────────
if __name__ == "__main__":
    tests = [
        ("RfS_for_1200MW_ISTS_Solar_PV.pdf",              "RfS"),
        ("Bid_Opening_Statement_SECI_2026.pdf",            "BidOpening"),
        ("Letter_of_Award_to_Adani_Green.pdf",             "LoA"),
        ("BOQ_for_Balance_of_System.pdf",                  "BOQ"),
        ("Corrigendum_No_2_Extension_of_Date.pdf",         "Corrigendum"),
        ("Pre-Bid_Meeting_Minutes_Queries.pdf",            "PreBid"),
        ("PPA_between_SECI_and_Developer.pdf",             "PPA"),
        ("random_document.pdf",                            "Other"),
    ]

    print("Testing classifier...\n")
    passed = 0
    for name, expected in tests:
        result = classify_by_name(name)
        ok = result == expected
        if ok:
            passed += 1
        status = "✓" if ok else "✗"
        print(f"  {status} classify_by_name('{name}')")
        print(f"      Expected: {expected} | Got: {result}")

    print(f"\n{passed}/{len(tests)} tests passed")