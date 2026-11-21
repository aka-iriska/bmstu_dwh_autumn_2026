from __future__ import annotations

from cinema_dwh.config import load_settings
from cinema_dwh.paths import CATALOGUE_KEY, s3a
from cinema_dwh.run_context import add_meta, drop_meta
from cinema_dwh.schemas import read_catalogue
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import dataset_path, log_summary, mart_path, run_meta, write_partitioned
from cinema_dwh.transforms.marts import build_all_marts
from cinema_dwh.transforms.scd2 import SCD2_COLUMNS


def run(**context) -> None:
    """Записать views_enriched и marts/<витрина>/ (с метаполями) для затронутых суток."""
    meta = run_meta(context)
    with spark_session("build_marts") as spark:
        clean = drop_meta(spark.read.parquet(dataset_path(meta.run_id, "clean_events")))
        scd2 = spark.read.parquet(dataset_path(meta.run_id, "user_profile_scd2")).select(*SCD2_COLUMNS)
        catalogue = read_catalogue(spark, s3a(CATALOGUE_KEY, load_settings().s3_bucket))
        enriched, marts = build_all_marts(clean, catalogue, scd2)
        write_partitioned(add_meta(enriched, meta), dataset_path(meta.run_id, "views_enriched"))
        counts = {}
        for name, mart in marts.items():
            # Витрины маленькие: один файл на витрину удобнее читать из ClickHouse и глазами.
            add_meta(mart, meta).coalesce(1).write.mode("overwrite").parquet(mart_path(meta.run_id, name))
            counts[name] = mart.count()
        log_summary("Витрины (строк)", counts)
