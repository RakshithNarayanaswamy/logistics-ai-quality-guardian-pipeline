import logging
import os
import subprocess
import sys

from dotenv import load_dotenv

from pipeline.config import BASE_DIR

load_dotenv(os.path.join(BASE_DIR, ".env"))


def run_dbt_models():
    dbt_dir = os.path.join(BASE_DIR, "logistics_dbt")
    dbt_bin = os.path.join(os.path.dirname(sys.executable), "dbt")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = dbt_dir

    result = subprocess.run(
        [dbt_bin, "build", "--project-dir", dbt_dir],
        cwd=dbt_dir,
        env=env,
        capture_output=True,
        text=True,
    )
    logging.info(result.stdout)
    if result.returncode != 0:
        logging.error(result.stderr)
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")
