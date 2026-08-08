import logging

from pipeline.metrics import compute_metrics
from pipeline.notifications import send_slack_message
from pipeline.reports import METRIC_REPORT, QUALITY_REPORT, get_latest_reports


def _format_digest(reports, metrics):
    lines = ["*Logistics pipeline — daily batch complete*", ""]

    headline = [
        f"OTIF: {metrics['otif_pct']}%",
        f"Avg lead time: {metrics['avg_lead_time_days']}d",
        f"Delay rate: {metrics['delay_rate_pct']}%",
        f"Stockout rate: {metrics['stockout_rate_pct']}%",
    ]
    lines.append(" | ".join(headline))
    lines.append("")

    quality = reports.get(QUALITY_REPORT)
    if quality:
        lines += ["*Pipeline health*", quality["content"], ""]

    narrative = reports.get(METRIC_REPORT)
    if narrative:
        lines += ["*Metrics*", narrative["content"]]

    return "\n".join(lines).strip()


def publish_reports():
    """Final DAG step: pushes the run's AI reports + headline metrics to Slack.

    The Streamlit dashboard reads Snowflake directly, so it needs no refresh —
    this task only handles the push notification.
    """
    reports = get_latest_reports()
    metrics = compute_metrics()

    digest = _format_digest(reports, metrics)
    sent = send_slack_message(digest)

    if not sent:
        logging.info("Digest built but not sent (no webhook configured):\n%s", digest)
    return digest
