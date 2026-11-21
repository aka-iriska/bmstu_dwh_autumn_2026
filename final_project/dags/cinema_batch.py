from datetime import datetime, timedelta, timezone
from importlib import import_module

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

TASKS = ["resolve_run_context",
         "freeze_snapshot",
         "prepare_data",
         "build_user_profile_scd2",
         "build_marts",
         "compare_rdd_df",
         "quality_checks",
         "publish_to_clickhouse",
         "advance_serving_boundary"]


def _callable(task_id: str):
    """Функция run задачи; модуль импортируется только при выполнении (pyspark не грузится при разборе DAG)."""
    # файл задачи назван по её порядку в TASKS: 1_resolve_run_context.py, 2_freeze_snapshot.py, ...
    # (имя с цифры нельзя написать в обычном import - поэтому грузим через import_module)
    module = f"cinema_dwh.tasks.{TASKS.index(task_id) + 1}_{task_id}"

    def call(**context):
        return import_module(module).run(**context)
    call.__name__ = task_id
    return call


def _on_failure(context):
    """Упавшая задача -> запуск получает статус failed в batch_runs."""
    from cinema_dwh.tasks.context import mark_run_failed
    mark_run_failed(context)


with DAG(
    dag_id="cinema_batch",
    # расписание - сутки UTC
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    # данные - январь 2026: без end_date планировщик сам запустил бы «вчера» по реальному календарю;
    end_date=datetime(2026, 1, 31, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,               # запуски строго по одному: граница T двигается последовательно
    params={
        "interval_start": Param(None, type=["null", "string"], description="ISO UTC, полночь; пусто - из расписания"),
        "interval_end": Param(None, type=["null", "string"], description="ISO UTC, полночь; пусто - из расписания"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "on_failure_callback": _on_failure},
    tags=["final_project", "batch"],
    doc_md="DWH онлайн-кинотеатра. Январские запуски: `airflow dags backfill cinema_batch -s 2026-01-01 -e 2026-01-01`; параметры `interval_start` / `interval_end` пересчитывают любой период.",
) as dag:
    ops = {task_id: PythonOperator(task_id=task_id, python_callable=_callable(task_id)) for task_id in TASKS}

    # resolve_run_context -> freeze_snapshot -> prepare_data -> build_user_profile_scd2 -> build_marts -> [compare_rdd_df, quality_checks] -> publish_to_clickhouse -> advance_serving_boundary
    (ops["resolve_run_context"] >> ops["freeze_snapshot"] >> ops["prepare_data"]
     >> ops["build_user_profile_scd2"] >> ops["build_marts"])
    ops["build_marts"] >> [ops["compare_rdd_df"], ops["quality_checks"]] >> ops["publish_to_clickhouse"]
    ops["publish_to_clickhouse"] >> ops["advance_serving_boundary"]
