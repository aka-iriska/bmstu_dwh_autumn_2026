from __future__ import annotations

import logging
from datetime import date, datetime, timezone

from cinema_dwh import s3io
from cinema_dwh.config import load_settings
from cinema_dwh.paths import dataset_key, mart_key, s3a, snapshot_key
from cinema_dwh.run_context import RunMeta, from_iso

# Общие помощники для задач DAG (файлы в cinema_dwh/tasks/).
# XCom - маленькие значения, которые одна задача Airflow передаёт следующим (return из run()).

log = logging.getLogger("airflow.task")   # строки попадают в лог попытки задачи в Airflow


def run_meta(context: dict) -> RunMeta:
    """Узнать, какой это запуск: run_id и расчётный интервал (их вернула первая задача resolve_run_context)."""
    rc = context["ti"].xcom_pull(task_ids="resolve_run_context")   # ti - «эта задача в этом запуске» у Airflow
    return RunMeta(rc["run_id"], from_iso(rc["interval_start"]), from_iso(rc["interval_end"]),
                   datetime.now(timezone.utc))   # processed_at - когда считает текущая задача


def affected(context: dict) -> list[date]:
    """Какие сутки пересчитывает запуск (их вернула задача prepare_data), например [2026-01-01]."""
    return [date.fromisoformat(d) for d in context["ti"].xcom_pull(task_ids="prepare_data")]


def read_snapshot(run_id: str) -> dict:
    """Прочитать из S3 файл batch/<run_id>/snapshot.json - список файлов архива, которые читает этот запуск."""
    return s3io.read_json(snapshot_key(run_id))


def spark_paths(keys: list[str]) -> list[str]:
    """Пути файлов в бакете (raw/events/...) -> адреса для Spark (s3a://cinema/raw/events/...)."""
    bucket = load_settings().s3_bucket
    return [s3a(key, bucket) for key in keys]


def dataset_path(run_id: str, name: str) -> str:
    """Адрес Parquet-папки запуска для Spark, например s3a://cinema/batch/<run_id>/clean_events/."""
    return spark_paths([dataset_key(run_id, name)])[0]


def mart_path(run_id: str, mart: str) -> str:
    """Адрес папки витрины запуска для Spark, например s3a://cinema/batch/<run_id>/marts/views_minute/."""
    return spark_paths([mart_key(run_id, mart)])[0]


def write_partitioned(df, path: str) -> None:
    """Записать таблицу Spark в Parquet по папкам event_date=YYYY-MM-DD/ (по одной папке на сутки)."""
    writer = df.write.mode("overwrite")   # повтор задачи перезаписывает, а не дописывает
    # Пустую таблицу с partitionBy Spark пишет вообще без файлов, и следующий шаг не сможет её прочитать -
    # поэтому пустую пишем одним файлом без папок по датам (в нём сохранится список колонок).
    (writer.parquet(path) if df.isEmpty() else writer.partitionBy("event_date").parquet(path))


def mark_run_failed(context: dict) -> None:
    """Вызывается Airflow, когда любая задача DAG упала: запуск получает статус failed в batch_runs."""
    from cinema_dwh import clickhouse   # импорт здесь: модуль грузится, только если что-то упало

    run_context = context["ti"].xcom_pull(task_ids="resolve_run_context")
    if run_context:   # если упала сама первая задача, run_id ещё не вычислен - записывать некуда
        clickhouse.record_run(clickhouse.client(), run_context["run_id"], status="failed")


def log_summary(title: str, values: dict) -> None:
    """Вывести в лог задачи итог столбиком: «название : значение» (например, сколько строк записано)."""
    width = max(len(k) for k in values)
    log.info("\n".join([title] + [f"    {k.ljust(width)} : {v}" for k, v in values.items()]))
