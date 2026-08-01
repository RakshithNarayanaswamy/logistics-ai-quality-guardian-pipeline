import logging
import os
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.config import QUARANTINE_DIR, QUARANTINE_THRESHOLD, RAW_DIR, STAGED_DIR
from pipeline.spark_utils import get_spark

_VALID_STATUSES = {"delivered", "in_transit", "delayed"}


def transform_data():
    spark = get_spark()
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
        if ratio > QUARANTINE_THRESHOLD:
            raise ValueError(
                f"Quarantine ratio {ratio:.1%} exceeds {QUARANTINE_THRESHOLD:.0%} threshold "
                f"({quarantine_count}/{total} rows quarantined)"
            )
    finally:
        spark.stop()
