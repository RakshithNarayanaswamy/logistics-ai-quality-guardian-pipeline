import os
import sys

# Must be set before pyspark is imported, so workers match the driver's interpreter.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402


def get_spark():
    return (
        SparkSession.builder
        .appName("logistics_pipeline")
        .master("local[*]")
        .getOrCreate()
    )
