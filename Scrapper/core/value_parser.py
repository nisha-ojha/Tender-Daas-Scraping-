"""
core/value_parser.py
--------------------
Parses Indian currency amounts from tender portals.

FORMATS YOU WILL ENCOUNTER:
  - "INR 1,42,50,000"         → Indian comma notation (Lakh/Crore grouping)
  - "Rs. 14250000"            → No commas
  - "₹ 1,42,50,000.00"       → With rupee symbol and decimals
  - "14.25 Crore"             → Written in Crore notation
  - "142.5 Lakh"              → Written in Lakh notation
  - "As per RfS"              → Non-numeric → store as NULL
  - "Nil"                     → Zero
  - "Not Applicable"          → NULL

INDIAN NUMBER SYSTEM REMINDER:
  1,00,000 = 1 Lakh = 100,000
  1,00,00,000 = 1 Crore = 10,000,000
"""

import re


def parse_amount(text):
    """
    Parse an Indian currency amount string into an integer (in Rupees).

    Args:
        text: Raw amount string from website

    Returns:
        Integer amount in Rupees, or None if non-numeric
    """
    if not text:
        return None

    original = text  # Keep original for debugging
    text = text.strip()

    # Handle non-numeric values
    lower = text.lower()
    if lower in ("nil", "0", "zero"):
        return 0
    if lower in ("na", "n/a", "not applicable", "-", "--", "as per rfs",
                  "as per tender document", "refer tender document"):
        return None

    # Check for Crore/Lakh notation first (e.g., "14.25 Crore")
    crore_match = re.search(r"([\d,.]+)\s*(?:crore|cr)", text, re.IGNORECASE)
    if crore_match:
        num_str = crore_match.group(1).replace(",", "")
        try:
            return int(float(num_str) * 10_000_000)  # 1 Crore = 10^7
        except ValueError:
            pass

    lakh_match = re.search(r"([\d,.]+)\s*(?:lakh|lac|l)", text, re.IGNORECASE)
    if lakh_match:
        num_str = lakh_match.group(1).replace(",", "")
        try:
            return int(float(num_str) * 100_000)  # 1 Lakh = 10^5
        except ValueError:
            pass

    # Remove currency prefixes: INR, Rs., Rs, ₹, Rs./-
    text = re.sub(r"(?:INR|Rs\.?/?-?|₹)\s*", "", text, flags=re.IGNORECASE)

    # Remove commas (handles both Indian and Western notation)
    text = text.replace(",", "")

    # Remove trailing ".00" or similar decimals
    text = re.sub(r"\.\d{1,2}$", "", text.strip())

    # Remove any remaining non-numeric chars (like spaces, /, -)
    text = re.sub(r"[^\d.]", "", text)

    if not text:
        return None

    try:
        return int(float(text))
    except ValueError:
        print(f"  [VALUE WARNING] Could not parse amount: '{original}'")
        return None


def format_inr(amount):
    """
    Format an integer amount into Indian Rupee display string.

    Examples:
        14250000 → "₹1.42 Crore"
        750000   → "₹7.50 Lakh"
        45000    → "₹45,000"
    """
    if amount is None:
        return "N/A"
    if amount == 0:
        return "₹0"

    abs_amount = abs(amount)

    if abs_amount >= 10_000_000:  # 1 Crore+
        crore = abs_amount / 10_000_000
        return f"₹{crore:.2f} Crore"
    elif abs_amount >= 100_000:  # 1 Lakh+
        lakh = abs_amount / 100_000
        return f"₹{lakh:.2f} Lakh"
    else:
        # Format with Indian comma notation for smaller amounts
        return f"₹{abs_amount:,}"


# ─── Quick self-test ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing value parser...")

    tests = [
        ("INR 1,42,50,000", 14250000),
        ("Rs. 14250000", 14250000),
        ("₹ 1,42,50,000.00", 14250000),
        ("14.25 Crore", 142500000),
        ("142.5 Lakh", 14250000),
        ("Rs. 2,00,000", 200000),
        ("Nil", 0),
        ("As per RfS", None),
        ("NA", None),
        ("", None),
        (None, None),
    ]

    passed = 0
    for raw, expected in tests:
        result = parse_amount(raw)
        status = "✓" if result == expected else "✗"
        if status == "✓":
            passed += 1
        print(f"  {status} parse_amount('{raw}') → {result} (expected: {expected})")

    print(f"\n{'✓' if passed == len(tests) else '✗'} Value parser: {passed}/{len(tests)} tests passed")

    # Test display formatter
    print("\nTesting display formatter...")
    display_tests = [
        (14250000, "₹1.42 Crore"),
        (142500000, "₹14.25 Crore"),
        (750000, "₹7.50 Lakh"),
        (45000, "₹45,000"),
        (None, "N/A"),
        (0, "₹0"),
    ]
    for amount, expected in display_tests:
        result = format_inr(amount)
        status = "✓" if result == expected else "✗"
        print(f"  {status} format_inr({amount}) → '{result}' (expected: '{expected}')")
