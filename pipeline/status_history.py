import logging
import os
import random
from datetime import datetime, timedelta

from pyspark.sql import types as T

from pipeline.config import STAGED_DIR
from pipeline.spark_utils import get_spark

_HISTORY_SCHEMA = T.StructType([
    T.StructField("shipment_id", T.StringType()),
    T.StructField("event_status", T.StringType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("event_sequence", T.IntegerType()),
])


def _row_to_events(row):
    order_date = row.order_date
    status = row.status

    if status == "delivered":
        end_date = row.actual_delivery_date or row.promised_delivery_date
        states = ["created", "picked", "packed", "shipped", "out_for_delivery", "delivered"]
    elif status == "delayed":
        end_date = row.promised_delivery_date
        states = ["created", "picked", "packed", "shipped"]
    else:  # in_transit
        end_date = row.promised_delivery_date
        states = ["created", "picked", "packed", "shipped", "out_for_delivery"]

    if end_date is None or end_date <= order_date:
        end_date = order_date + timedelta(days=1)

    total_span_days = (end_date - order_date).days or 1
    n = len(states)
    start = datetime.combine(order_date, datetime.min.time())

    events = []
    for i, state in enumerate(states):
        frac = i / max(n - 1, 1)
        offset_days = frac * total_span_days
        jitter_hours = random.uniform(-2, 2)
        event_ts = start + timedelta(days=offset_days, hours=jitter_hours)
        events.append((row.shipment_id, state, event_ts.isoformat(), i))
    return events


def extract_status_history():
    spark = get_spark()
    try:
        staged_path = os.path.join(STAGED_DIR, "shipments.parquet")
        shipments = spark.read.parquet(staged_path).select(
            "shipment_id", "order_date", "promised_delivery_date",
            "actual_delivery_date", "status",
        )

        events_rdd = shipments.rdd.flatMap(_row_to_events)
        events_df = spark.createDataFrame(events_rdd, schema=_HISTORY_SCHEMA)

        output_path = os.path.join(STAGED_DIR, "status_history.parquet")
        events_df.write.mode("overwrite").parquet(output_path)

        count = events_df.count()
        logging.info("Generated %d status-history events to %s", count, output_path)
    finally:
        spark.stop()
