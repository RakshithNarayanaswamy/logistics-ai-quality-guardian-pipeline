import json
import logging
import os
import urllib.request

from dotenv import load_dotenv

from pipeline.config import BASE_DIR

load_dotenv(os.path.join(BASE_DIR, ".env"))

_TIMEOUT_SECONDS = 15


def send_slack_message(text):
    """Post to the incoming webhook in SLACK_WEBHOOK_URL.

    No-ops with a warning when the webhook isn't configured, so the DAG
    doesn't fail just because notifications aren't set up on this machine.
    Returns True if a message was actually sent.
    """
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        logging.warning("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return False

    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
        body = response.read().decode().strip()

    if body != "ok":
        raise RuntimeError(f"Slack webhook returned an unexpected response: {body!r}")

    logging.info("Slack notification sent (%d chars)", len(text))
    return True
