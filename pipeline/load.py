import logging
import os

from dotenv import load_dotenv

from pipeline.config import (
    BASE_DIR,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TABLE,
    STAGED_DIR,
)
from pipeline.snowflake_utils import get_connection, stage_and_copy

load_dotenv(os.path.join(BASE_DIR, ".env"))

_CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        shipment_id STRING,
        part_name STRING,
        carrier_name STRING,
        quantity INTEGER,
        cost FLOAT,
        shipment_date DATE,
        status STRING,
        warehouse_id STRING,
        order_date DATE,
        promised_delivery_date DATE,
        actual_delivery_date DATE
    )
"""


def load_to_snowflake():
    conn = get_connection(SNOWFLAKE_SCHEMA)
    cur = conn.cursor()
    try:
        staged_path = os.path.join(STAGED_DIR, "shipments.parquet")
        result = stage_and_copy(
            cur, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE,
            staged_path, _CREATE_SQL,
        )
        logging.info("load_to_snowflake — COPY INTO result: %s", result)
    finally:
        cur.close()
        conn.close()
