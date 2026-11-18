"""Daily diagnostics pipeline. Each task calls the same function the CLI uses."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from diaglab import pipeline

DATA_DIR = "/opt/airflow/data"


def _detect_and_file(**_):
    regs = pipeline.step_detect(DATA_DIR)
    return pipeline.step_file_bugs(DATA_DIR, regs)


with DAG(
    dag_id="diagnostics_daily",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["diagnostics"],
) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=pipeline.step_ingest,
                            op_kwargs={"data_dir": DATA_DIR})
    aggregate = PythonOperator(task_id="aggregate", python_callable=pipeline.step_aggregate,
                               op_kwargs={"data_dir": DATA_DIR})
    detect_and_file = PythonOperator(task_id="detect_and_file_bugs", python_callable=_detect_and_file)
    ingest >> aggregate >> detect_and_file
