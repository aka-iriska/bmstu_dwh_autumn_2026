from datetime import date

from airflow.models import DagBag

from tests.conftest import PROJECT


def _dag():
    bag = DagBag(dag_folder=str(PROJECT / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.dags["cinema_batch"]   # get_dag полез бы в базу метаданных, она тесту не нужна


def test_dag_structure():
    dag = _dag()
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.end_date.date() == date(2026, 1, 31)
    assert set(dag.params.keys()) == {"interval_start", "interval_end"}
    assert set(dag.get_task("publish_to_clickhouse").upstream_task_ids) == {"compare_rdd_df", "quality_checks"}
    assert dag.get_task("advance_serving_boundary").upstream_task_ids == {"publish_to_clickhouse"}
    assert dag.get_task("freeze_snapshot").upstream_task_ids == {"resolve_run_context"}
    assert len(dag.tasks) == 9


def test_resolve_interval_params_override():
    from importlib import import_module

    resolve_interval = import_module("cinema_dwh.tasks.1_resolve_run_context")._resolve_interval
    from tests.helpers import dt

    start, end = resolve_interval({"interval_start": "2026-01-01T00:00:00Z", "interval_end": "2026-01-04T00:00:00Z"},
                                  dt("2026-01-10"), dt("2026-01-11"))
    assert (start, end) == (dt("2026-01-01"), dt("2026-01-04"))
    assert resolve_interval({}, dt("2026-01-02"), dt("2026-01-03")) == (dt("2026-01-02"), dt("2026-01-03"))
