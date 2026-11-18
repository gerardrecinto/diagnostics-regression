import os

import pytest

pytest.importorskip("airflow")


def test_dag_loads_and_orders_tasks():
    from airflow.models import DagBag
    folder = os.path.join(os.path.dirname(__file__), "..", "dags")
    bag = DagBag(dag_folder=folder, include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("diagnostics_daily")
    assert dag.catchup is False and dag.max_active_runs == 1
    assert [t.task_id for t in dag.topological_sort()] == ["ingest", "aggregate", "detect_and_file_bugs"]
