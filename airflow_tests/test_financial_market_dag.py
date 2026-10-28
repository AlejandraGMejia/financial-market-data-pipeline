"""Unit tests for the financial market Airflow DAG."""

import importlib.util
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAG_FILE = PROJECT_ROOT / "dags" / "financial_market_dag.py"


def load_dag_module():
    """Import the DAG without starting Airflow services."""
    spec = importlib.util.spec_from_file_location(
        "financial_market_dag",
        DAG_FILE,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load the financial market DAG.")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestFinancialMarketDag(unittest.TestCase):
    """Validate DAG configuration and subprocess behavior."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_dag_module()
        cls.task = cls.module.dag.task_dict["run_market_etl"]

    def test_dag_configuration(self):
        self.assertEqual(
            self.module.dag.dag_id,
            "financial_market_etl",
        )
        self.assertIsNone(self.module.dag.schedule)
        self.assertFalse(self.module.dag.catchup)
        self.assertEqual(self.module.dag.max_active_runs, 1)
        self.assertEqual(
            set(self.module.dag.task_ids),
            {"run_market_etl"},
        )

    def test_task_executes_offline_pipeline(self):
        with (
            patch.object(
                Path,
                "is_file",
                return_value=True,
            ),
            patch.object(
                self.module.subprocess,
                "run",
            ) as mock_run,
        ):
            self.task.python_callable()

        mock_run.assert_called_once()

        args, kwargs = mock_run.call_args

        self.assertEqual(
            args[0],
            [
                str(self.module.ETL_PYTHON),
                str(self.module.PIPELINE_SCRIPT),
                "--source",
                "raw",
                "--symbols",
                "AAPL",
            ],
        )
        self.assertEqual(
            kwargs["cwd"],
            self.module.PROJECT_ROOT,
        )
        self.assertTrue(kwargs["check"])
        self.assertEqual(kwargs["timeout"], 240)

    def test_missing_etl_environment_raises_error(self):
        with (
            patch.object(
                Path,
                "is_file",
                return_value=False,
            ),
            self.assertRaises(FileNotFoundError),
        ):
            self.task.python_callable()

    def test_subprocess_failure_is_propagated(self):
        with (
            patch.object(
                Path,
                "is_file",
                return_value=True,
            ),
            patch.object(
                self.module.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(
                    1,
                    ["python", "run_pipeline.py"],
                ),
            ),
            self.assertRaises(subprocess.CalledProcessError),
        ):
            self.task.python_callable()


if __name__ == "__main__":
    unittest.main()
