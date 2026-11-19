from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable


def build_user_scd2(**ctx):
    from task1_user_scd2 import main          # /opt/airflow/jobs в PYTHONPATH; pyspark импортируется только в задаче
    return main(**ctx)                  # в XCom уходит только путь к снимку


def build_campaign_recipients(scd2_path, **ctx):
    from task2_campaign_recipients import main
    return main(scd2_path, **ctx)

# ДЗ 2.3: build_user_scd2 -> build_campaign_recipients, ежедневно в 00:00 UTC за сутки [start, end)
with DAG(
    dag_id="hw23_scd2_campaigns",
    # ежедневно в 00:00 UTC, каждый запуск обрабатывает завершённые сутки [data_interval_start, data_interval_end).
    # CronDataIntervalTimetable даёт интервал ровно в сутки
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    # c 23 по 25 января 2026
    start_date=datetime(2026, 1, 23, tzinfo=timezone.utc),
    end_date=datetime(2026, 1, 25, tzinfo=timezone.utc),
    # scheduler сам создаёт и выполняет исторические запуски
    catchup=True,
    # max_active_runs=1
    max_active_runs=1,
    # хотя бы одна автоматическая повторная попытка при ошибке
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["hw23"],
) as dag:
    scd2 = PythonOperator(task_id="build_user_scd2", python_callable=build_user_scd2)
    recipients = PythonOperator(
        task_id="build_campaign_recipients",
        python_callable=build_campaign_recipients,
        # между задачами передаём только путь к результату (не содержимое таблиц);
        # xcom_pull в рамках этого же DAG run -> значит снимок того же D
        op_kwargs={"scd2_path": "{{ ti.xcom_pull(task_ids='build_user_scd2') }}"},
    )
    # По условию вторая задача запускается только после успешного завершения первой в том же DAG run
    scd2 >> recipients
