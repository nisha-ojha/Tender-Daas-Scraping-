"""
core/alerts.py
--------------
Sends webhook alerts to Discord / Slack / Telegram when things happen.

HOW IT WORKS:
  1. You create a webhook URL on Discord (or Slack/Telegram)
  2. Put that URL in your .env file: WEBHOOK_URL=https://discord.com/api/webhooks/...
  3. This code sends a formatted message to that URL whenever called

IMPORTANT: This module should NEVER crash your scraper.
If Discord is down, we just print the error and move on.
"""

import os
import json
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")


def send_alert(status, portal, message, batch_id=None, extra_fields=None):
    """
    Send an alert via webhook.

    Args:
        status: 'success', 'error', 'warning', or 'info'
        portal: 'seci', 'cppp', etc.
        message: Human-readable description of what happened
        batch_id: Pipeline run ID for tracking
        extra_fields: Optional list of dicts [{"name": "...", "value": "..."}]
    """
    # Always print to console (even if webhook is not configured)
    icon = {"success": "✓", "error": "✗", "warning": "⚠", "info": "ℹ"}
    print(f"  [{icon.get(status, '?')} {status.upper()}] {portal}: {message}")

    # If no webhook URL configured, just return after printing
    if not WEBHOOK_URL:
        return

    # Color codes for Discord embeds
    color_map = {
        "success": 3066993,   # Green
        "error": 15158332,    # Red
        "warning": 15844367,  # Yellow
        "info": 3447003,      # Blue
    }

    # Build Discord embed payload
    fields = [
        {"name": "Portal", "value": portal.upper(), "inline": True},
        {"name": "Status", "value": status.upper(), "inline": True},
        {"name": "Time", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "inline": True},
    ]

    if batch_id:
        fields.append({"name": "Batch ID", "value": f"`{batch_id}`", "inline": False})

    if extra_fields:
        fields.extend(extra_fields)

    payload = {
        "embeds": [
            {
                "title": f"{status.upper()}: {portal.upper()} Scraper",
                "description": message,
                "color": color_map.get(status, 0),
                "fields": fields,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ]
    }

    # Send the webhook — wrapped in try/except so it NEVER crashes the scraper
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"  [ALERT WARNING] Webhook returned status {resp.status_code}")
    except requests.exceptions.Timeout:
        print("  [ALERT WARNING] Webhook timed out (Discord may be slow)")
    except requests.exceptions.ConnectionError:
        print("  [ALERT WARNING] Cannot reach webhook URL. Check your internet.")
    except Exception as e:
        print(f"  [ALERT WARNING] Webhook failed: {e}")


# ─── Convenience functions ───────────────────────────────────

def alert_success(portal, message, batch_id=None, **kwargs):
    send_alert("success", portal, message, batch_id, **kwargs)

def alert_error(portal, message, batch_id=None, **kwargs):
    send_alert("error", portal, message, batch_id, **kwargs)

def alert_warning(portal, message, batch_id=None, **kwargs):
    send_alert("warning", portal, message, batch_id, **kwargs)

def alert_info(portal, message, batch_id=None, **kwargs):
    send_alert("info", portal, message, batch_id, **kwargs)


# ─── Quick self-test ─────────────────────────────────────────
if __name__ == "__main__":
    print("Testing alert system...")

    if WEBHOOK_URL:
        print(f"Webhook URL configured: {WEBHOOK_URL[:50]}...")
        send_alert(
            status="info",
            portal="test",
            message="This is a test alert from Tender DAAS. If you see this in Discord, alerts are working!",
            batch_id="test_run_001",
        )
        print("✓ Test alert sent! Check your Discord channel.")
    else:
        print("⚠ No WEBHOOK_URL in .env — alerts will only print to console.")
        print("  This is fine for now. Set it up later when you have Discord ready.")
        send_alert("info", "test", "Console-only test alert")
        print("✓ Console alert test PASSED!")
