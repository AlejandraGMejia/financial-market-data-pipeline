"""Airflow DAG for the financial market ETL pipeline."""

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ETL_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
PIPELINE_SCRIPT = PROJECT_ROOT / "run_pipeline.py"


with DAG(
    dag_id="financial_market_etl",
    description="Orchestrate the financial market ETL pipeline",
    start_date=pendulum.datetime(2026, 10, 26, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "data-engineering"],
) as dag:

    @task(retries=0, execution_timeout=timedelta(minutes=5))
    def run_market_etl() -> None:
        """Execute the existing ETL using its own virtual environment."""

        if not ETL_PYTHON.is_file():
            raise FileNotFoundError("ETL virtual environment was not found.")

        command = [
            str(ETL_PYTHON),
            str(PIPELINE_SCRIPT),
            "--source",
            "raw",
            "--symbols",
            "AAPL",
        ]

        environment = os.environ.copy()

        subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=environment,
            check=True,
            timeout=240,
        )

    run_market_etl()
