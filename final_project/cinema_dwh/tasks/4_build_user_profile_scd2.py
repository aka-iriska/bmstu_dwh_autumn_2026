from __future__ import annotations

from cinema_dwh.run_context import add_meta, drop_meta
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import dataset_path, log_summary, run_meta
from cinema_dwh.transforms.scd2 import build_scd2

# Шаг 4 DAG: история профилей (SCD2) - какая страна и тариф были у пользователя в каждый момент времени.
# Читает: batch/<run_id>/clean_profile_changes.
# Пишет: batch/<run_id>/user_profile_scd2.


def run(**context) -> None:
    """Прочитать clean_profile_changes, построить версии, записать user_profile_scd2 с метаполями."""
    meta = run_meta(context=context)
    input_path = dataset_path(run_id=meta.run_id, name="clean_profile_changes")
    output_path = dataset_path(run_id=meta.run_id, name="user_profile_scd2")
    with spark_session(app_name="build_user_profile_scd2") as spark:
        changes_with_meta = spark.read.parquet(input_path)
        # Метаполя (run_id, interval_*, processed_at) убираем: build_scd2 работает только с полями профиля.
        changes = drop_meta(df=changes_with_meta)
        # Версии [valid_from, valid_to) по всей истории.
        versions = build_scd2(changes=changes)
        # Снова добавляем метаполя - уже этого запуска.
        scd2 = add_meta(df=versions, meta=meta)
        # Таблица маленькая (пользователей мало) - один файл вместо десятков мелких.
        scd2.coalesce(1).write.mode("overwrite").parquet(output_path)
        versions_count = scd2.count()
        users_count = scd2.select("user_id").distinct().count()
        log_summary(title="SCD2", values={"versions": versions_count, "users": users_count})
