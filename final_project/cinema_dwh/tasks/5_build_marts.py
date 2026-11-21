from __future__ import annotations

from cinema_dwh.config import load_settings
from cinema_dwh.paths import CATALOGUE_KEY, s3a
from cinema_dwh.run_context import add_meta, drop_meta
from cinema_dwh.schemas import read_catalogue
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import dataset_path, log_summary, mart_path, run_meta, write_partitioned
from cinema_dwh.transforms.marts import build_all_marts
from cinema_dwh.transforms.scd2 import SCD2_COLUMNS

# Шаг 5 DAG: JOIN просмотров с каталогом и историей профилей, затем витрины.
# Читает: clean_events, user_profile_scd2, catalogue.csv.
# Пишет: views_enriched и marts/<витрина>/.


def run(**context) -> None:
    """Записать views_enriched и marts/<витрина>/ (с метаполями) для затронутых суток."""
    meta = run_meta(context=context)
    clean_events_path = dataset_path(run_id=meta.run_id, name="clean_events")
    scd2_path = dataset_path(run_id=meta.run_id, name="user_profile_scd2")
    enriched_path = dataset_path(run_id=meta.run_id, name="views_enriched")
    catalogue_path = s3a(key=CATALOGUE_KEY, bucket=load_settings().s3_bucket)   # статичный файл каталога в S3
    with spark_session(app_name="build_marts") as spark:
        # События только за затронутые сутки (их отобрал шаг 3). Метаполя убираем,
        # иначе после JOIN будет две колонки run_id.
        clean = drop_meta(df=spark.read.parquet(clean_events_path))
        # Из SCD2 берём только поля профиля (user_id, country, tier, valid_from, valid_to) - по той же причине.
        scd2 = spark.read.parquet(scd2_path).select(*SCD2_COLUMNS)
        catalogue = read_catalogue(spark=spark, path=catalogue_path)
        # enriched = просмотры + категория фильма + страна/тариф на момент просмотра; marts = все витрины.
        enriched, marts = build_all_marts(clean_events=clean, catalogue=catalogue, scd2=scd2)
        # views_enriched раскладываем по папкам event_date=... - его проверяет шаг 7.
        write_partitioned(df=add_meta(df=enriched, meta=meta), path=enriched_path)
        counts = {}
        for name, mart in marts.items():
            mart_with_meta = add_meta(df=mart, meta=meta)
            # Витрины маленькие: один файл на витрину удобнее читать из ClickHouse и глазами.
            mart_with_meta.coalesce(1).write.mode("overwrite").parquet(mart_path(run_id=meta.run_id, mart=name))
            counts[name] = mart.count()
        log_summary(title="Витрины (строк)", values=counts)
