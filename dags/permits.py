"""Optional orchestration: every task calls the standalone pipeline interface."""
from datetime import timedelta
import hashlib
import subprocess
import sys

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def execute_stage(stage, run_id, **_):
    # Airflow run ids contain punctuation; hashing keeps paths safe and retry-stable.
    pipeline_run_id = "airflow-" + hashlib.sha256(run_id.encode()).hexdigest()[:24]
    subprocess.run(
        [sys.executable, "-m", "pipeline.cli", stage,
         "--config", "configs/seattle.json", "--run-id", pipeline_run_id,
         "--data-dir", "/data"],
        cwd="/opt/project", check=True,
    )
    return pipeline_run_id


with DAG(
    dag_id="seattle_permits",
    description="Seattle permits: standalone commands orchestrated by Airflow",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Los_Angeles"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["learning", "spark", "seattle"],
) as dag:
    stages = [
        PythonOperator(
            task_id=stage,
            python_callable=execute_stage,
            op_kwargs={"stage": stage},
        )
        for stage in ("extract", "validate", "transform", "rates", "publish")
    ]
    for previous, following in zip(stages, stages[1:]):
        previous >> following
