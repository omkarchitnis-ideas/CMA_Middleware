import os
import time
import json
import logging
import threading
from datetime import datetime
import requests

logger = logging.getLogger("teams_notifier")

# Cache to prevent alert spamming (de-duplication)
_ALERT_HISTORY = {}
_ALERT_LOCK = threading.Lock()
COOLDOWN_SECONDS = 300  # 5 minutes cooldown for identical alerts

def _send_webhook_request(webhook_url, payload):
    try:
        resp = requests.post(webhook_url, json=payload, timeout=8)
        if resp.status_code in (200, 202):
            logger.info("Teams alert dispatched successfully.")
        else:
            logger.warning(f"Teams alert returned HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.error(f"Failed to post alert to Teams webhook: {e}")

def send_teams_alert(service_name, title, message, severity="INFO", details=None, force=False):
    """
    Dispatches a structured Adaptive Card alert to Microsoft Teams asynchronously.
    
    :param service_name: Name of the reporting service (e.g. 'CMA Middleware', 'SFDC Middleware')
    :param title: Short title of the alert
    :param message: Detailed explanation of the event or issue
    :param severity: 'INFO', 'SUCCESS', 'WARNING', or 'CRITICAL'
    :param details: Optional dict of key-value pairs to display as card facts
    :param force: If True, bypasses the 5-minute spam suppression cooldown
    """
    webhook_url = os.getenv("TEAMS_MIDDLEWARE_WEBHOOK_URL") or os.getenv("TEAMS_WEBHOOK_URL")
    if not webhook_url:
        logger.debug("No TEAMS_MIDDLEWARE_WEBHOOK_URL configured. Alert skipped.")
        return

    # Severity-based styling
    sev_upper = severity.upper()
    if sev_upper in ("CRITICAL", "ERROR"):
        color = "Attention"  # Red
        emoji = "🔴"
    elif sev_upper == "WARNING":
        color = "Warning"    # Yellow
        emoji = "🟡"
    elif sev_upper in ("SUCCESS", "RECOVERED"):
        color = "Good"       # Green
        emoji = "🟢"
    else:
        color = "Accent"     # Blue
        emoji = "ℹ️"

    # De-duplication check
    alert_key = f"{service_name}:{title}:{sev_upper}"
    now = time.time()
    with _ALERT_LOCK:
        last_sent = _ALERT_HISTORY.get(alert_key, 0)
        if not force and (now - last_sent < COOLDOWN_SECONDS):
            logger.debug(f"Suppressing duplicate Teams alert '{alert_key}' (cooldown active: {int(COOLDOWN_SECONDS - (now - last_sent))}s remaining).")
            return
        _ALERT_HISTORY[alert_key] = now

    # Format timestamp
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Build Adaptive Card Body
    body_elements = [
        {
            "type": "Container",
            "items": [
                {
                    "type": "TextBlock",
                    "text": f"{emoji} [{service_name}] {title}",
                    "weight": "Bolder",
                    "size": "Medium",
                    "color": color
                },
                {
                    "type": "TextBlock",
                    "text": message,
                    "wrap": True,
                    "spacing": "Small"
                }
            ]
        }
    ]

    # Build Facts Table
    facts = [
        {"title": "Service:", "value": service_name},
        {"title": "Severity:", "value": sev_upper},
        {"title": "Timestamp:", "value": current_time_str}
    ]

    if details and isinstance(details, dict):
        for k, v in details.items():
            facts.append({"title": f"{k}:", "value": str(v)})

    body_elements.append({
        "type": "FactSet",
        "facts": facts,
        "spacing": "Medium"
    })

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": body_elements
                }
            }
        ]
    }

    # Dispatch in background daemon thread (non-blocking)
    threading.Thread(target=_send_webhook_request, args=(webhook_url, payload), daemon=True).start()
