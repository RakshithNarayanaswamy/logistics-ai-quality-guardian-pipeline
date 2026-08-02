"""Deliberately breaks RAW.SHIPMENTS to test the quality guardian's detection.

Each scenario here corresponds to one of pipeline/quality_checks.py's 4 checks.
Not part of the daily DAG — run manually:

    python -m pipeline.drift_injector <scenario> [options]
    python -m pipeline.drift_injector revert

Scenarios:
    row_count_drop                          delete ~80% of rows
    null_spike --column NAME --fraction F    null out a column on a sample of rows
    new_status --count N                     set N rows to an out-of-set status
    schema_drop_column --column NAME         destructive: drop a column
    schema_add_column --column NAME          additive: add a new column
    revert                                    undo whatever was last injected
"""
import argparse
import logging

from pipeline.config import SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE
from pipeline.load import load_to_snowflake
from pipeline.snowflake_utils import get_connection

_QUALIFIED_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"


def inject_row_count_drop():
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {_QUALIFIED_TABLE} WHERE UNIFORM(0::float, 1::float, RANDOM()) < 0.8"
        )
        logging.info("Deleted ~80%% of rows from %s (rows affected: %d)",
                     _QUALIFIED_TABLE, cur.rowcount)
    finally:
        cur.close()
        conn.close()


def inject_null_spike(column="carrier_name", fraction=0.15):
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE {_QUALIFIED_TABLE} SET {column} = NULL "
            f"WHERE UNIFORM(0::float, 1::float, RANDOM()) < {fraction}"
        )
        logging.info("Nulled out '%s' on ~%.0f%% of rows (rows affected: %d)",
                     column, fraction * 100, cur.rowcount)
    finally:
        cur.close()
        conn.close()


def inject_new_status(count=100):
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE {_QUALIFIED_TABLE}
            SET status = 'returned'
            WHERE shipment_id IN (
                SELECT shipment_id FROM {_QUALIFIED_TABLE} LIMIT {count}
            )
        """)
        logging.info("Set status='returned' on %d rows (out-of-set value)", cur.rowcount)
    finally:
        cur.close()
        conn.close()


def inject_schema_drop_column(column="carrier_name"):
    """Destructive — downstream dbt models that explicitly select this column
    will start failing until it's restored via revert().
    """
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {_QUALIFIED_TABLE} DROP COLUMN {column}")
        logging.info("Dropped column '%s' from %s (destructive)", column, _QUALIFIED_TABLE)
    finally:
        cur.close()
        conn.close()


def inject_schema_add_column(column="tracking_pin", coltype="STRING"):
    """Additive — harmless to downstream dbt models, which only select known
    columns by name.
    """
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {_QUALIFIED_TABLE} ADD COLUMN {column} {coltype}")
        logging.info("Added column '%s %s' to %s (additive)", column, coltype, _QUALIFIED_TABLE)
    finally:
        cur.close()
        conn.close()


def revert():
    """Undoes every scenario above. Row deletes/null spikes/bad statuses are
    fixed by a normal reload (TRUNCATE + COPY INTO from the last staged
    parquet). A dropped column needs to be dropped-and-recreated at the table
    level first, since CREATE TABLE IF NOT EXISTS won't alter an existing table.
    """
    conn = get_connection(SNOWFLAKE_SCHEMA)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_QUALIFIED_TABLE}")
        logging.info("Dropped %s to clear any schema drift", _QUALIFIED_TABLE)
    finally:
        cur.close()
        conn.close()

    load_to_snowflake()
    logging.info("Reverted: %s recreated and reloaded from the last staged batch",
                 _QUALIFIED_TABLE)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="scenario", required=True)

    sub.add_parser("row_count_drop")

    p = sub.add_parser("null_spike")
    p.add_argument("--column", default="carrier_name")
    p.add_argument("--fraction", type=float, default=0.15)

    p = sub.add_parser("new_status")
    p.add_argument("--count", type=int, default=100)

    p = sub.add_parser("schema_drop_column")
    p.add_argument("--column", default="carrier_name")

    p = sub.add_parser("schema_add_column")
    p.add_argument("--column", default="tracking_pin")
    p.add_argument("--coltype", default="STRING")

    sub.add_parser("revert")

    args = parser.parse_args()

    if args.scenario == "row_count_drop":
        inject_row_count_drop()
    elif args.scenario == "null_spike":
        inject_null_spike(args.column, args.fraction)
    elif args.scenario == "new_status":
        inject_new_status(args.count)
    elif args.scenario == "schema_drop_column":
        inject_schema_drop_column(args.column)
    elif args.scenario == "schema_add_column":
        inject_schema_add_column(args.column, args.coltype)
    elif args.scenario == "revert":
        revert()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
