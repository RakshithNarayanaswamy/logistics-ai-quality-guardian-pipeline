import logging

from ai.claude_client import ask_claude
from pipeline.metrics import compute_metrics
from pipeline.reports import METRIC_REPORT, save_report

_SYSTEM_PROMPT = (
    "You are a supply-chain metrics assistant for a logistics shipments pipeline. "
    "You are given computed metrics covering: OTIF %, average lead time, per-stage "
    "shipment status transition times (which stage is the slowest), current "
    "warehouse stockout rate, per-carrier performance/cost, a 7-day demand forecast "
    "by part, and delay/exception rates overall and by carrier. Write a concise "
    "5-7 sentence narrative summary highlighting the most notable patterns (e.g. "
    "worst-performing carrier, the slowest transition stage, any stockout risk, any "
    "part with a sharp forecasted change). Never invent numbers not present in the "
    "input — only narrate what's given. Reply with plain prose only: no markdown "
    "headings, no bullet lists, no preamble — the text is rendered directly into a "
    "dashboard panel and a Slack message."
)


def run_metric_assistant():
    """AI layer, Role 2: narrates metrics that dbt/SQL already computed. The LLM
    never computes the numbers itself — it only interprets what pipeline.metrics
    already found.
    """
    metrics = compute_metrics()
    report = ask_claude(_SYSTEM_PROMPT, str(metrics))
    logging.info("metric_assistant report:\n%s", report)
    save_report(METRIC_REPORT, report)
    return report
