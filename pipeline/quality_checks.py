import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from pipeline.config import BASE_DIR, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE
from pipeline.snowflake_utils import get_connection

load_dotenv(os.path.join(BASE_DIR, ".env"))

_HISTORY_TABLE = f"{SNOWFLAKE_DATABASE}.ANALYTICS.PIPELINE_QUALITY_HISTORY"
_MONITORED_NULLABLE_COLS = ["part_name", "carrier_name", "warehouse_id"]
_EXPECTED_COLUMNS = {
    "shipment_id", "part_name", "carrier_name", "quantity", "cost",
    "shipment_date", "status", "warehouse_id", "order_date",
    "promised_delivery_date", "actual_delivery_date",
}


def _qualified_table():
    return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"


def _current_columns(cur):
    cur.execute(f"DESCRIBE TABLE {_qualified_table()}")
    return {row[0].lower() for row in cur.fetchall()}


def _compute_batch_stats(cur):
    cur.execute(f"SELECT COUNT(*) FROM {_qualified_table()}")
    row_count = cur.fetchone()[0]

    null_rates = {}
    for col in _MONITORED_NULLABLE_COLS:
        cur.execute(f"""
            SELECT SUM(IFF({col} IS NULL, 1, 0))::FLOAT / NULLIF(COUNT(*), 0)
            FROM {_qualified_table()}
        """)
        null_rates[col] = cur.fetchone()[0] or 0.0

    cur.execute(f"SELECT DISTINCT status FROM {_qualified_table()}")
    distinct_statuses = sorted(r[0] for r in cur.fetchall())

    return {
        "row_count": row_count,
        "null_rates": null_rates,
        "distinct_statuses": distinct_statuses,
        "columns": sorted(_current_columns(cur)),
    }


def _ensure_history_table(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_HISTORY_TABLE} (
            run_ts TIMESTAMP_NTZ,
            row_count NUMBER,
            null_rates VARIANT,
            distinct_statuses VARIANT,
            columns VARIANT
        )
    """)


def _persist_stats(cur, stats):
    cur.execute(
        f"""
            INSERT INTO {_HISTORY_TABLE} (run_ts, row_count, null_rates, distinct_statuses, columns)
            SELECT %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s)
        """,
        (
            datetime.now(timezone.utc).replace(tzinfo=None),
            stats["row_count"],
            json.dumps(stats["null_rates"]),
            json.dumps(stats["distinct_statuses"]),
            json.dumps(stats["columns"]),
        ),
    )


def _fetch_previous_run(cur):
    cur.execute(f"""
        SELECT row_count, null_rates, distinct_statuses, columns
        FROM {_HISTORY_TABLE}
        ORDER BY run_ts DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "row_count": row[0],
        "null_rates": json.loads(row[1]),
        "distinct_statuses": json.loads(row[2]),
        "columns": json.loads(row[3]),
    }


def _diff_against_previous(current, previous):
    findings = []
    if previous is None:
        return findings

    if previous["row_count"]:
        row_delta_pct = (
            (current["row_count"] - previous["row_count"]) / previous["row_count"] * 100
        )
        if abs(row_delta_pct) > 20:
            findings.append(
                f"Row count changed {row_delta_pct:+.1f}% vs previous run "
                f"({previous['row_count']} -> {current['row_count']})"
            )

    for col in _MONITORED_NULLABLE_COLS:
        prev_rate = previous["null_rates"].get(col, 0.0)
        curr_rate = current["null_rates"][col]
        if curr_rate - prev_rate > 0.02:
            findings.append(
                f"Null rate for '{col}' rose from {prev_rate:.1%} to {curr_rate:.1%}"
            )

    new_statuses = set(current["distinct_statuses"]) - set(previous["distinct_statuses"])
    if new_statuses:
        findings.append(f"New status value(s) seen: {sorted(new_statuses)}")

    missing_cols = set(previous["columns"]) - set(current["columns"])
    added_cols = set(current["columns"]) - set(previous["columns"])
    if missing_cols:
        findings.append(f"Column(s) disappeared since last run: {sorted(missing_cols)}")
    if added_cols:
        findings.append(f"New column(s) appeared since last run: {sorted(added_cols)}")

    return findings


def run_quality_checks():
    """Deterministic-only: computes this batch's stats, diffs against the prior run,
    persists the new stats for next time. No AI here — see ai/quality_guardian.py
    for the narration layer built on top of this.
    """
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        _ensure_history_table(cur)

        previous = _fetch_previous_run(cur)
        current = _compute_batch_stats(cur)

        findings = _diff_against_previous(current, previous)

        expected_missing = _EXPECTED_COLUMNS - set(current["columns"])
        if expected_missing:
            findings.append(f"Expected column(s) missing from schema: {sorted(expected_missing)}")

        _persist_stats(cur, current)
        conn.commit()

        logging.info("run_quality_checks — current=%s findings=%s", current, findings)
        return {"current": current, "previous": previous, "findings": findings}
    finally:
        cur.close()
        conn.close()
