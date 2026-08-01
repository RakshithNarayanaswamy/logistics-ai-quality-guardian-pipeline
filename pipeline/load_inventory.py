import logging
import os

from dotenv import load_dotenv

from pipeline.config import BASE_DIR, RAW_DIR, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
from pipeline.snowflake_utils import get_connection, stage_and_copy

load_dotenv(os.path.join(BASE_DIR, ".env"))

_TABLE = "INVENTORY_SNAPSHOTS"
_CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{_TABLE} (
        snapshot_date DATE,
        part_name STRING,
        warehouse_id STRING,
        warehouse_inventory INTEGER,
        inventory_cost_per_unit FLOAT
    )
"""


def load_inventory_to_snowflake():
    conn = get_connection(SNOWFLAKE_SCHEMA)
    cur = conn.cursor()
    try:
        local_path = os.path.join(RAW_DIR, "inventory_snapshots.parquet")
        result = stage_and_copy(
            cur, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, _TABLE, local_path, _CREATE_SQL,
        )
        logging.info("load_inventory_to_snowflake — COPY INTO result: %s", result)
    finally:
        cur.close()
        conn.close()
