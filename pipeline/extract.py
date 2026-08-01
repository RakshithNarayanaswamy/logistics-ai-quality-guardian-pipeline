import logging
import os
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import types as T

from pipeline.config import RAW_DIR, RECORD_COUNT
from pipeline.spark_utils import get_spark

_CARRIERS = ["FedEx", "UPS", "DHL", "XPO Logistics", "Maersk"]
_PARTS = ["Battery Pack", "Motor Assembly", "Chassis Frame", "Control Unit", "Sensor Array"]
_STATUSES = ["delivered", "in_transit", "delayed"]

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


def extract_shipments():
    spark = get_spark()
    try:
        rows = spark.sparkContext.parallelize(range(RECORD_COUNT)).map(_random_row)
        df = spark.createDataFrame(rows, schema=_RAW_SCHEMA)

        output_path = os.path.join(RAW_DIR, "shipments.parquet")
        df.write.mode("overwrite").parquet(output_path)

        logging.info("Extracted %d shipment records to %s", RECORD_COUNT, output_path)
    finally:
        spark.stop()
