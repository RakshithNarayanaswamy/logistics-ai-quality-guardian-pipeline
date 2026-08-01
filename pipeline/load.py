import logging
import os

import snowflake.connector
from dotenv import load_dotenv

from pipeline.config import (
    BASE_DIR,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TABLE,
    STAGED_DIR,
)

load_dotenv(os.path.join(BASE_DIR, ".env"))


def load_to_snowflake():
    qualified_table = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
    stage = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}_STAGE"
    staged_path = os.path.join(STAGED_DIR, "shipments.parquet")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE STAGE IF NOT EXISTS {stage}")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {qualified_table} (
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
        """)
        # Full-refresh load for now (each run replaces the table). Once dbt takes over
        # incremental/partitioned loads, this can move to an append-only landing pattern.
        cur.execute(f"TRUNCATE TABLE {qualified_table}")

        cur.execute(
            f"PUT 'file://{staged_path}/part-*.parquet' @{stage} "
            f"OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
        )

        cur.execute(f"""
            COPY INTO {qualified_table}
            FROM @{stage}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
        """)
        copy_result = cur.fetchall()
        logging.info("load_to_snowflake — COPY INTO result: %s", copy_result)
    finally:
        cur.close()
        conn.close()
