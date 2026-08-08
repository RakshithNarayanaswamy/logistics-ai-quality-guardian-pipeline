import logging
from datetime import datetime, timezone

from pipeline.config import SNOWFLAKE_DATABASE
from pipeline.snowflake_utils import get_connection

_ANALYTICS_SCHEMA = "ANALYTICS"
_REPORTS_TABLE = f"{SNOWFLAKE_DATABASE}.{_ANALYTICS_SCHEMA}.PIPELINE_REPORTS"

QUALITY_REPORT = "quality_guardian"
METRIC_REPORT = "metric_assistant"


def _ensure_table(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_REPORTS_TABLE} (
            run_ts TIMESTAMP_NTZ,
            report_type STRING,
            content STRING
        )
    """)


def save_report(report_type, content):
    """Persist an AI-generated report so it outlives the Airflow task log —
    gives the dashboard and the Slack digest something durable to read, and
    builds a history of what the pipeline said on previous days.
    """
    conn = get_connection(_ANALYTICS_SCHEMA)
    cur = conn.cursor()
    try:
        _ensure_table(cur)
        cur.execute(
            f"INSERT INTO {_REPORTS_TABLE} (run_ts, report_type, content) "
            f"VALUES (%s, %s, %s)",
            (datetime.now(timezone.utc).replace(tzinfo=None), report_type, content),
        )
        conn.commit()
        logging.info("Saved '%s' report to %s", report_type, _REPORTS_TABLE)
    finally:
        cur.close()
        conn.close()


def get_latest_reports():
    """Most recent report of each type, keyed by report_type."""
    conn = get_connection(_ANALYTICS_SCHEMA)
    cur = conn.cursor()
    try:
        _ensure_table(cur)
        cur.execute(f"""
            SELECT report_type, content, run_ts
            FROM {_REPORTS_TABLE}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY report_type ORDER BY run_ts DESC
            ) = 1
        """)
        return {r[0]: {"content": r[1], "run_ts": r[2]} for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def get_report_history(report_type, limit=10):
    """Previous runs of one report type, newest first."""
    conn = get_connection(_ANALYTICS_SCHEMA)
    cur = conn.cursor()
    try:
        _ensure_table(cur)
        cur.execute(
            f"SELECT run_ts, content FROM {_REPORTS_TABLE} "
            f"WHERE report_type = %s ORDER BY run_ts DESC LIMIT {int(limit)}",
            (report_type,),
        )
        return [{"run_ts": r[0], "content": r[1]} for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()
