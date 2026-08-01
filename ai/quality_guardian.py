import logging

from ai.claude_client import ask_claude
from pipeline.quality_checks import run_quality_checks

_SYSTEM_PROMPT = (
    "You are a data pipeline quality guardian for a logistics shipments batch pipeline. "
    "You are given deterministic check results (row counts, null rates, schema, status "
    "values) comparing the latest batch to the previous run. Write a concise 2-4 sentence "
    "plain-English health report. If there are no findings, say the batch looks healthy. "
    "Never invent numbers not present in the input — only narrate what's given."
)


def run_quality_guardian():
    """AI layer, Role 1: narrates the deterministic check results from
    pipeline.quality_checks. The LLM never decides pass/fail — it only explains
    what the deterministic logic already found.
    """
    result = run_quality_checks()
    findings = result["findings"]

    user_prompt = (
        f"Current batch stats: {result['current']}\n"
        f"Findings vs previous run: {findings if findings else 'none'}"
    )
    report = ask_claude(_SYSTEM_PROMPT, user_prompt)
    logging.info("quality_guardian report:\n%s", report)
    return report
