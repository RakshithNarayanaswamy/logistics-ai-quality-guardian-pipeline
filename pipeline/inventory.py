import logging
import os
import random
from datetime import date, timedelta

from pyspark.sql import types as T

from pipeline.catalog import WAREHOUSE_IDS, get_part_catalog
from pipeline.config import RAW_DIR
from pipeline.spark_utils import get_spark

_PARTS = get_part_catalog()
_N_DATES = 730  # ~2 years of daily snapshots
_STOCKOUT_PROBABILITY = 0.05
_END_DATE = date.today()

_TOTAL_ROWS = _N_DATES * len(_PARTS) * len(WAREHOUSE_IDS)

_INVENTORY_SCHEMA = T.StructType([
    T.StructField("snapshot_date", T.StringType()),
    T.StructField("part_name", T.StringType()),
    T.StructField("warehouse_id", T.StringType()),
    T.StructField("warehouse_inventory", T.IntegerType()),
    T.StructField("inventory_cost_per_unit", T.DoubleType()),
])


def _cost_per_unit(part_name):
    # Deterministic per part (not per row) — a given SKU has a stable unit cost
    # across warehouses/dates, same as a real cost catalog would.
    rng = random.Random(hash(part_name) % (2**32))
    return round(rng.uniform(5, 500), 2)


def _random_row(idx):
    date_idx = idx % _N_DATES
    part_idx = (idx // _N_DATES) % len(_PARTS)
    wh_idx = idx // (_N_DATES * len(_PARTS))

    part_name = _PARTS[part_idx]
    warehouse_id = WAREHOUSE_IDS[wh_idx]
    snapshot_date = _END_DATE - timedelta(days=date_idx)

    if random.random() < _STOCKOUT_PROBABILITY:
        quantity = 0
    else:
        quantity = random.randint(1, 5000)

    return (
        snapshot_date.isoformat(),
        part_name,
        warehouse_id,
        quantity,
        _cost_per_unit(part_name),
    )


def extract_inventory_snapshots():
    spark = get_spark()
    try:
        rows = spark.sparkContext.parallelize(range(_TOTAL_ROWS)).map(_random_row)
        df = spark.createDataFrame(rows, schema=_INVENTORY_SCHEMA)

        output_path = os.path.join(RAW_DIR, "inventory_snapshots.parquet")
        df.write.mode("overwrite").parquet(output_path)

        logging.info(
            "Extracted %d inventory snapshot rows (%d parts x %d warehouses x %d days) to %s",
            _TOTAL_ROWS, len(_PARTS), len(WAREHOUSE_IDS), _N_DATES, output_path,
        )
    finally:
        spark.stop()
