import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGED_DIR = os.path.join(BASE_DIR, "data", "staged")
QUARANTINE_DIR = os.path.join(BASE_DIR, "data", "quarantine")

for _dir in (RAW_DIR, STAGED_DIR, QUARANTINE_DIR):
    os.makedirs(_dir, exist_ok=True)

# Dev-mode toggle: iterate on a small sample locally, flip to full scale later.
RECORD_COUNT = int(os.environ.get("LOGISTICS_RECORD_COUNT", "10000"))
QUARANTINE_THRESHOLD = 0.10

SNOWFLAKE_DATABASE = "LOGISTICS_PIPELINE"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_TABLE = "SHIPMENTS"
