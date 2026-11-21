from __future__ import annotations

from cinema_dwh.run_context import add_meta, drop_meta
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import dataset_path, log_summary, run_meta
from cinema_dwh.transforms.scd2 import build_scd2


def run(**context) -> None:
    """Прочитать clean_profile_changes, построить версии, записать user_profile_scd2 с метаполями."""
    meta = run_meta(context)
    with spark_session("build_user_profile_scd2") as spark:
        changes = drop_meta(spark.read.parquet(dataset_path(meta.run_id, "clean_profile_changes")))
        scd2 = add_meta(build_scd2(changes), meta)
        scd2.coalesce(1).write.mode("overwrite").parquet(dataset_path(meta.run_id, "user_profile_scd2"))
        log_summary("SCD2", {"versions": scd2.count(), "users": scd2.select("user_id").distinct().count()})
