from __future__ import annotations

from cinema_dwh import s3io
from cinema_dwh.checks import assert_all_ok, run_quality_checks
from cinema_dwh.config import load_settings
from cinema_dwh.paths import CATALOGUE_KEY, MARTS, checks_key, s3a
from cinema_dwh.schemas import read_catalogue
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import dataset_path, log_summary, mart_path, run_meta

# Шаг 7 DAG (параллельно с шагом 6): проверки качества перед публикацией.
# Читает: clean_events, каталог, user_profile_scd2, views_enriched, все витрины.
# Пишет: batch/<run_id>/checks/quality.json. Хоть одна проверка не прошла -> задача падает, публикации не будет.


def run(**context) -> None:
    """Проверить ключи витрин, SCD2, JOIN и суммы; записать checks/quality.json."""
    meta = run_meta(context=context)
    catalogue_path = s3a(key=CATALOGUE_KEY, bucket=load_settings().s3_bucket)
    with spark_session(app_name="quality_checks") as spark:
        clean_events = spark.read.parquet(dataset_path(run_id=meta.run_id, name="clean_events"))
        catalogue = read_catalogue(spark=spark, path=catalogue_path)
        scd2 = spark.read.parquet(dataset_path(run_id=meta.run_id, name="user_profile_scd2"))
        enriched = spark.read.parquet(dataset_path(run_id=meta.run_id, name="views_enriched"))
        marts = {name: spark.read.parquet(mart_path(run_id=meta.run_id, mart=name)) for name in MARTS}
        # Сами проверки - в checks.py: уникальность ключей, одна версия профиля на момент,
        # каждый просмотр нашёл фильм и профиль, суммы витрин сходятся с views_enriched.
        report = run_quality_checks(clean_events=clean_events, catalogue=catalogue, scd2=scd2,
                                    enriched=enriched, marts=marts)
    # Отчёт пишем до проверки результата - он нужен как раз тогда, когда что-то упало.
    s3io.write_json(key=checks_key(run_id=meta.run_id, name="quality"), obj={"run_id": meta.run_id, **report})
    summary = {c["name"]: "ok" if c["ok"] else f"FAILED {c['details']}" for c in report["checks"]}
    log_summary(title="Проверки качества", values=summary)
    # Падение здесь останавливает DAG: шаг 8 (публикация) не запустится.
    assert_all_ok(report=report)
