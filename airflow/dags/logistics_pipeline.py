import logging
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Dev-mode toggle: iterate on a small sample locally, flip to full scale later.
RECORD_COUNT = int(os.environ.get("LOGISTICS_RECORD_COUNT", "10000"))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGED_DIR = os.path.join(BASE_DIR, "data", "staged")
QUARANTINE_DIR = os.path.join(BASE_DIR, "data", "quarantine")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGED_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

_CARRIERS = ["FedEx", "UPS", "DHL", "XPO Logistics", "Maersk"]
_PARTS = ["Battery Pack", "Motor Assembly", "Chassis Frame", "Control Unit", "Sensor Array"]
_STATUSES = ["delivered", "in_transit", "delayed"]
_VALID_STATUSES = set(_STATUSES)
_QUARANTINE_THRESHOLD = 0.10


def _get_spark():
    return (
        SparkSession.builder
        .appName("logistics_pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def _random_row(_):
    order_date = datetime.today() - timedelta(days=random.randint(1, 45))
    lead_time = random.randint(2, 14)
    promised_date = order_date + timedelta(days=lead_time)
    status = random.choice(_STATUSES)
    # Only delivered shipments have a real actual_delivery_date.
    if status == "delivered":
        actual_date = promised_date + timedelta(days=random.randint(-2, 5))
    else:
        actual_date = None

    return (
        str(uuid.uuid4()),
        random.choice(_PARTS),
        random.choice(_CARRIERS),
        random.randint(1, 500),
        round(random.uniform(100, 50000), 2),
        (order_date + timedelta(days=random.randint(0, lead_time))).date().isoformat(),
        status,
        f"WH-{random.randint(1, 5)}",
        order_date.date().isoformat(),
        promised_date.date().isoformat(),
        actual_date.date().isoformat() if actual_date else None,
    )


_RAW_SCHEMA = T.StructType([
    T.StructField("shipment_id", T.StringType()),
    T.StructField("part_name", T.StringType()),
    T.StructField("carrier_name", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("cost", T.DoubleType()),
    T.StructField("shipment_date", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("order_date", T.StringType()),
    T.StructField("promised_delivery_date", T.StringType()),
    T.StructField("actual_delivery_date", T.StringType()),
])


def extract_shipments():
    spark = _get_spark()
    try:
        rows = spark.sparkContext.parallelize(range(RECORD_COUNT)).map(_random_row)
        df = spark.createDataFrame(rows, schema=_RAW_SCHEMA)

        output_path = os.path.join(RAW_DIR, "shipments.parquet")
        df.write.mode("overwrite").parquet(output_path)

        logging.info("Extracted %d shipment records to %s", RECORD_COUNT, output_path)
    finally:
        spark.stop()


def transform_data():
    spark = _get_spark()
    try:
        raw_path = os.path.join(RAW_DIR, "shipments.parquet")
        df = spark.read.parquet(raw_path).withColumn(
            "quantity_coerced", F.col("quantity").cast(T.IntegerType())
        ).withColumn(
            "cost_coerced", F.col("cost").cast(T.DoubleType())
        ).withColumn(
            "shipment_date_coerced", F.to_date("shipment_date")
        ).withColumn(
            "order_date_coerced", F.to_date("order_date")
        ).withColumn(
            "promised_delivery_date_coerced", F.to_date("promised_delivery_date")
        ).withColumn(
            "actual_delivery_date_coerced", F.to_date("actual_delivery_date")
        )

        total = df.count()

        dup_ids = (
            df.groupBy("shipment_id").count().filter(F.col("count") > 1)
            .select("shipment_id")
        )

        df = df.withColumn(
            "reason",
            F.concat_ws(
                "; ",
                F.when(F.col("shipment_id").isNull() | (F.trim("shipment_id") == ""),
                       F.lit("shipment_id is null")),
                F.when(F.col("shipment_id").isin([r["shipment_id"] for r in dup_ids.collect()]),
                       F.lit("shipment_id is duplicate")),
                F.when(F.col("quantity_coerced").isNull(),
                       F.lit("quantity cannot be coerced to int")),
                F.when(F.col("quantity_coerced").isNotNull() & (F.col("quantity_coerced") <= 0),
                       F.lit("quantity is not a positive integer")),
                F.when(F.col("cost_coerced").isNull(),
                       F.lit("cost cannot be coerced to float")),
                F.when(F.col("cost_coerced").isNotNull() & (F.col("cost_coerced") < 0),
                       F.lit("cost is negative")),
                F.when(~F.col("status").isin(sorted(_VALID_STATUSES)),
                       F.lit(f"status not in {sorted(_VALID_STATUSES)}")),
                F.when(F.col("shipment_date_coerced").isNull(),
                       F.lit("shipment_date cannot be parsed as a date")),
                F.when(F.col("order_date_coerced").isNull(),
                       F.lit("order_date cannot be parsed as a date")),
                F.when(F.col("promised_delivery_date_coerced").isNull(),
                       F.lit("promised_delivery_date cannot be parsed as a date")),
            ),
        )

        df_invalid = df.filter(F.col("reason") != "")
        df_valid = (
            df.filter(F.col("reason") == "")
            .drop("reason")
            .drop("quantity", "cost", "shipment_date", "order_date",
                  "promised_delivery_date", "actual_delivery_date")
            .withColumnRenamed("quantity_coerced", "quantity")
            .withColumnRenamed("cost_coerced", "cost")
            .withColumnRenamed("shipment_date_coerced", "shipment_date")
            .withColumnRenamed("order_date_coerced", "order_date")
            .withColumnRenamed("promised_delivery_date_coerced", "promised_delivery_date")
            .withColumnRenamed("actual_delivery_date_coerced", "actual_delivery_date")
        )
        df_invalid = df_invalid.select(
            "shipment_id", "part_name", "carrier_name", "quantity", "cost",
            "shipment_date", "status", "warehouse_id", "order_date",
            "promised_delivery_date", "actual_delivery_date", "reason",
        )

        valid_count = df_valid.count()
        quarantine_count = df_invalid.count()
        logging.info("transform_data — total=%d  valid=%d  quarantined=%d",
                     total, valid_count, quarantine_count)

        staged_path = os.path.join(STAGED_DIR, "shipments.parquet")
        df_valid.write.mode("overwrite").parquet(staged_path)

        if quarantine_count > 0:
            run_date = datetime.today().strftime("%Y-%m-%d")
            quarantine_path = os.path.join(QUARANTINE_DIR, f"shipments_{run_date}.parquet")
            df_invalid.write.mode("overwrite").parquet(quarantine_path)

        ratio = quarantine_count / total
        if ratio > _QUARANTINE_THRESHOLD:
            raise ValueError(
                f"Quarantine ratio {ratio:.1%} exceeds {_QUARANTINE_THRESHOLD:.0%} threshold "
                f"({quarantine_count}/{total} rows quarantined)"
            )
    finally:
        spark.stop()

def load_to_snowflake():
    print("Loading transformed data into Snowflake")

def run_dbt_models():
    print("Running dbt models on top of loaded data")

def refresh_dashboard():
    print("Refreshing logistics dashboard")

with DAG(
    dag_id = "logistics_pipeline",
    start_date = datetime(2024, 1, 1),
    schedule_interval = "0 6 * * *",
    catchup = False,
) as dag:
    
    t1 = PythonOperator(task_id="extract_shipments", python_callable=extract_shipments)
    t2 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t3 = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake)
    t4 = PythonOperator(task_id="run_dbt_models", python_callable=run_dbt_models)
    t5 = PythonOperator(task_id="refresh_dashboard", python_callable=refresh_dashboard)

    t1 >> t2 >> t3 >> t4 >> t5