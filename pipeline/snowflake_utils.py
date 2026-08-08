import os

import snowflake.connector
from dotenv import load_dotenv

from pipeline.config import BASE_DIR, SNOWFLAKE_DATABASE

# Loaded here because this is the only module that reads the Snowflake env vars.
# Callers (DAG tasks, the Streamlit dashboard) then don't each need to remember to.
load_dotenv(os.path.join(BASE_DIR, ".env"))


def get_connection(schema):
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        database=SNOWFLAKE_DATABASE,
        schema=schema,
    )


def stage_and_copy(cur, database, schema, table, local_dir, create_table_sql,
                    file_glob="part-*.parquet"):
    """Never row-by-row: stage the local Parquet part-files, then COPY INTO.
    Full-refresh (TRUNCATE first) — matches the batch's overwrite-per-run pattern.
    """
    qualified_table = f"{database}.{schema}.{table}"
    stage = f"{database}.{schema}.{table}_STAGE"

    cur.execute(f"CREATE STAGE IF NOT EXISTS {stage}")
    cur.execute(create_table_sql)
    cur.execute(f"TRUNCATE TABLE {qualified_table}")
    cur.execute(
        f"PUT 'file://{local_dir}/{file_glob}' @{stage} OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
    )
    cur.execute(f"""
        COPY INTO {qualified_table}
        FROM @{stage}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE
    """)
    return cur.fetchall()
