source "$(dirname "${BASH_SOURCE[0]}")/airflow-env/bin/activate"
conda deactivate
export AIRFLOW_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/airflow"
echo "Environment ready. AIRFLOW_HOME set to $AIRFLOW_HOME"
