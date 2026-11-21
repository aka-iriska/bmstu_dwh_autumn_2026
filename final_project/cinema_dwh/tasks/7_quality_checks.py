from __future__ import annotations

from cinema_dwh import s3io
from cinema_dwh.checks import assert_all_ok, run_quality_checks
from cinema_dwh.config import load_settings
from cinema_dwh.paths import CATALOGUE_KEY, MARTS, checks_key, s3a
from cinema_dwh.schemas import read_catalogue
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import dataset_path, log_summary, mart_path, run_meta


def run(**context) -> None:
    """Проверить ключи витрин, SCD2, JOIN и суммы; записать checks/quality.json."""
    meta = run_meta(context)
    with spark_session("quality_checks") as spark:
        read = spark.read.parquet
        report = run_quality_checks(
            read(dataset_path(meta.run_id, "clean_events")),
            read_catalogue(spark, s3a(CATALOGUE_KEY, load_settings().s3_bucket)),
            read(dataset_path(meta.run_id, "user_profile_scd2")),
            read(dataset_path(meta.run_id, "views_enriched")),
            {name: read(mart_path(meta.run_id, name)) for name in MARTS},
        )
    s3io.write_json(checks_key(meta.run_id, "quality"), {"run_id": meta.run_id, **report})
    log_summary("Проверки качества", {c["name"]: "ok" if c["ok"] else f"FAILED {c['details']}" for c in report["checks"]})
    assert_all_ok(report)
