
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_BIN="$PROJECT_ROOT/.venv-airflow/bin"

if [[ ! -x "$AIRFLOW_BIN/airflow" ]]; then
    echo "ERROR: Airflow virtual environment was not found."
    exit 1
fi

if [[ ! -f "$PROJECT_ROOT/dags/financial_market_dag.py" ]]; then
    echo "ERROR: Financial market DAG was not found."
    exit 1
fi

export AIRFLOW_HOME="$HOME/.local/share/financial-market-airflow"
export PATH="$AIRFLOW_BIN:$PATH"

export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=1

export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:admin"
export AIRFLOW__API__HOST="127.0.0.1"

export PROJECT_ROOT

export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST="$(
    "$AIRFLOW_BIN/python" - <<'PY'
import json
import os
from pathlib import Path

dag_folder = Path(os.environ["PROJECT_ROOT"]) / "dags"

print(json.dumps([{
    "name": "dags-folder",
    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
    "kwargs": {"path": str(dag_folder)},
}]))
PY
)"

mkdir -p "$AIRFLOW_HOME"

echo "Starting Financial Market Airflow..."
echo "DAG folder: $PROJECT_ROOT/dags"
echo "Dashboard: http://127.0.0.1:8080"

exec airflow standalone
