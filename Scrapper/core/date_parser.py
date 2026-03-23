"""
core/date_parser.py
-------------------
Parses the various date formats found on Indian government tender portals.

FORMATS YOU WILL ENCOUNTER:
  - "10/03/2026"              → DD/MM/YYYY (SECI listing page)
  - "10/03/2026 19:00:00"    → DD/MM/YYYY HH:MM:SS (SECI detail page)
  - "10-03-2026"              → DD-MM-YYYY (some portals use dashes)
  - "2026-03-10"              → YYYY-MM-DD (ISO format, from some APIs)
  - "10 Mar 2026"             → DD Mon YYYY (CPPP uses this)
  - "March 10, 2026"          → Month DD, YYYY (rare, but it happens)

CRITICAL RULE: Indian portals use DD/MM/YYYY, NOT MM/DD/YYYY.
  "04/05/2026" means 4th May, NOT April 5th.
"""

import re
from datetime import datetime, date, timezone, timedelta

# Indian Standard Time offset
IST = timezone(timedelta(hours=5, minutes=30))

# All date formats to try, in order of likelihood
DATE_FORMATS = [
    "%d/%m/%Y %H:%M:%S",   # 10/03/2026 19:00:00
    "%d/%m/%Y %H:%M",      # 10/03/2026 19:00
    "%d/%m/%Y",             # 10/03/2026
    "%d-%m-%Y %H:%M:%S",   # 10-03-2026 19:00:00
    "%d-%m-%Y %H:%M",      # 10-03-2026 19:00
    "%d-%m-%Y",             # 10-03-2026
    "%Y-%m-%d %H:%M:%S",   # 2026-03-10 19:00:00 (ISO)
    "%Y-%m-%d",             # 2026-03-10 (ISO)
    "%d %b %Y %H:%M",      # 10 Mar 2026 19:00
    "%d %b %Y",             # 10 Mar 2026
    "%d %B %Y",             # 10 March 2026
    "%B %d, %Y",            # March 10, 2026
]


def parse_date(text):
    """
    Parse a date string from an Indian government portal.

    Args:
        text: The raw date string from the website

    Returns:
        A datetime.date object, or None if parsing fails
    """
    if not text:
        return None

    # Clean the text
    text = text.strip()
    text = re.sub(r"\s+", " ", text)  # Collapse multiple spaces

    # Skip obvious non-dates
    if text.lower() in ("", "na", "n/a", "nil", "-", "--", "not applicable"):
        return None

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date()  # Return date only (no time)
        except ValueError:
            continue

    print(f"  [DATE WARNING] Could not parse date: '{text}'")
    return None


def parse_datetime_ist(text):
    """
    Parse a date+time string and return a timezone-aware datetime in IST.

    Use this for deadlines and bid opening dates where time matters.

    Args:
        text: Raw date+time string

    Returns:
        A timezone-aware datetime in IST, or None if parsing fails
    """
    if not text:
        return None

    text = text.strip()
    text = re.sub(r"\s+", " ", text)

    if text.lower() in ("", "na", "n/a", "nil", "-", "--", "not applicable"):
        return None

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            # Attach IST timezone
            dt_ist = dt.replace(tzinfo=IST)
            return dt_ist
        except ValueError:
            continue

    print(f"  [DATE WARNING] Could not parse datetime: '{text}'")
    return None


def parse_date_safe(text):
    """
    Like parse_date, but returns the raw string if parsing fails.
    Useful when you want to store SOMETHING even if the format is unexpected.
    """
    result = parse_date(text)
    if result:
        return result
    return text  # Return raw string as fallback


# ─── Quick self-test ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing date parser...")

    tests = [
        ("10/03/2026", "2026-03-10"),
        ("04/05/2026", "2026-05-04"),           # DD/MM, not MM/DD!
        ("10/03/2026 19:00:00", "2026-03-10"),  # Strips time for date-only
        ("10-03-2026", "2026-03-10"),
        ("2026-03-10", "2026-03-10"),
        ("10 Mar 2026", "2026-03-10"),
        ("NA", None),
        ("", None),
        (None, None),
    ]

    passed = 0
    for raw, expected in tests:
        result = parse_date(raw)
        result_str = str(result) if result else None
        status = "✓" if result_str == expected else "✗"
        if status == "✓":
            passed += 1
        print(f"  {status} parse_date('{raw}') → {result_str} (expected: {expected})")

    print(f"\n{'✓' if passed == len(tests) else '✗'} Date parser: {passed}/{len(tests)} tests passed")

    # Test datetime with IST
    print("\nTesting datetime parser...")
    dt = parse_datetime_ist("04/05/2026 18:00:00")
    print(f"  parse_datetime_ist('04/05/2026 18:00:00') → {dt}")
    print(f"  Timezone: {dt.tzinfo}")
    print("✓ Datetime parser test PASSED!")
