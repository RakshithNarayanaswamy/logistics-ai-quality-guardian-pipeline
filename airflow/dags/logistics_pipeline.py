import os
import sys
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for _path in (_PROJECT_ROOT, os.path.join(_PROJECT_ROOT, "ml")):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import demand_forecasting  # noqa: E402

from ai.metric_assistant import run_metric_assistant  # noqa: E402
from ai.quality_guardian import run_quality_guardian  # noqa: E402
from pipeline.dbt_runner import run_dbt_models  # noqa: E402
from pipeline.extract import extract_shipments  # noqa: E402
from pipeline.inventory import extract_inventory_snapshots  # noqa: E402
from pipeline.load import load_to_snowflake  # noqa: E402
from pipeline.load_inventory import load_inventory_to_snowflake  # noqa: E402
from pipeline.load_status_history import load_status_history_to_snowflake  # noqa: E402
from pipeline.publish import publish_reports  # noqa: E402
from pipeline.status_history import extract_status_history  # noqa: E402
from pipeline.transform import transform_data  # noqa: E402


def train_demand_forecast():
    demand_forecasting.run()


with DAG(
    dag_id = "logistics_pipeline",
    start_date = datetime(2024, 1, 1),
    schedule = "0 6 * * *",
    catchup = False,
) as dag:

    t1 = PythonOperator(task_id="extract_shipments", python_callable=extract_shipments)
    t2 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t3 = PythonOperator(
        task_id="extract_status_history", python_callable=extract_status_history
    )
    t4 = PythonOperator(
        task_id="extract_inventory_snapshots", python_callable=extract_inventory_snapshots
    )
    t5 = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_snowflake)
    t6 = PythonOperator(
        task_id="load_status_history_to_snowflake",
        python_callable=load_status_history_to_snowflake,
    )
    t7 = PythonOperator(
        task_id="load_inventory_to_snowflake", python_callable=load_inventory_to_snowflake
    )
    t8 = PythonOperator(task_id="run_dbt_models", python_callable=run_dbt_models)
    t9 = PythonOperator(task_id="train_demand_forecast", python_callable=train_demand_forecast)
    t10 = PythonOperator(task_id="quality_guardian", python_callable=run_quality_guardian)
    t11 = PythonOperator(task_id="metric_assistant", python_callable=run_metric_assistant)
    t12 = PythonOperator(task_id="publish_reports", python_callable=publish_reports)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10 >> t11 >> t12
