import logging
import os

from dotenv import load_dotenv

from pipeline.config import BASE_DIR, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, STAGED_DIR
from pipeline.snowflake_utils import get_connection, stage_and_copy

load_dotenv(os.path.join(BASE_DIR, ".env"))

_TABLE = "SHIPMENT_STATUS_HISTORY"
_CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{_TABLE} (
        shipment_id STRING,
        event_status STRING,
        event_ts TIMESTAMP_NTZ,
        event_sequence INTEGER
    )
"""


def load_status_history_to_snowflake():
    conn = get_connection(SNOWFLAKE_SCHEMA)
    cur = conn.cursor()
    try:
        local_path = os.path.join(STAGED_DIR, "status_history.parquet")
        result = stage_and_copy(
            cur, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, _TABLE, local_path, _CREATE_SQL,
        )
        logging.info("load_status_history_to_snowflake — COPY INTO result: %s", result)
    finally:
        cur.close()
        conn.close()
