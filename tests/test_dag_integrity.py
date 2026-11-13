"""DAG integrity tests: the cheapest tests that catch the most common Airflow bugs.

They only *parse* the DAG files (like the dag-processor does): no database,
no scheduler, no network.
"""

from __future__ import annotations

import pytest
from airflow.dag_processing.dagbag import DagBag
from conftest import PROJECT_ROOT

EXPECTED_DAGS = {"ingest_ecb_rates", "ingest_nbu_rates", "build_fx_marts"}


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(PROJECT_ROOT / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_are_present(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED_DAGS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dag_conventions(dagbag, dag_id):
    dag = dagbag.dags[dag_id]  # .get_dag() would query the metadata DB
    assert dag.tags, "every DAG needs tags (filtering in the UI)"
    assert dag.doc_md, "every DAG needs documentation (shown in the UI)"
    assert dag.catchup is False
    for t in dag.tasks:
        assert t.owner == "data-engineering", t.task_id
        assert t.retries >= 1, f"{t.task_id} has no retries"


def test_ingest_dags_produce_assets_consumed_by_marts(dagbag):
    ecb_out = dagbag.dags["ingest_ecb_rates"].get_task("load_to_core").outlets
    nbu_out = dagbag.dags["ingest_nbu_rates"].get_task("load_to_core").outlets
    marts_schedule = dagbag.dags["build_fx_marts"].timetable.asset_condition

    consumed = {a.name for a in marts_schedule.objects}
    assert {a.name for a in ecb_out} | {a.name for a in nbu_out} == consumed


def test_ecb_task_graph(dagbag):
    dag = dagbag.dags["ingest_ecb_rates"]
    assert dag.get_task("load_to_core").upstream_task_ids == {
        "extract_to_raw",
        "create_table",
    }
    assert dag.get_task("extract_to_raw").upstream_task_ids == {"wait_for_publication"}
    assert dag.get_task("wait_for_publication").mode == "reschedule"


def test_marts_branching(dagbag):
    dag = dagbag.dags["build_fx_marts"]
    assert dag.get_task("route_alerts").downstream_task_ids == {
        "report_anomalies",
        "no_anomalies",
    }
