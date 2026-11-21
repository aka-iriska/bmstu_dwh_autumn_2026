from __future__ import annotations

from collections import defaultdict
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cinema_dwh import clickhouse
from cinema_dwh.paths import RAW_EVENTS_PREFIX, RAW_PROFILES_PREFIX
from cinema_dwh.run_context import RunMeta, add_meta
from cinema_dwh.schemas import read_events, read_profile_changes
from cinema_dwh.snapshot import affected_dates
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import (dataset_path, log_summary, read_snapshot, run_meta, spark_paths,
                                      write_partitioned)
from cinema_dwh.transforms.events import (dedup_events, dedup_profile_changes, validate_events,
                                          validate_profile_changes)

# Шаг 2 DAG: читает файлы архива из среза, отбраковывает плохие строки, убирает повторы,
# решает, какие сутки пересчитать, и пишет очищенные данные в batch/<run_id>/.

MAX_IDS_IN_REASON = 10   # причины в batch_runs читают люди - длинные списки ID не нужны


def _dates_with_ids(df: DataFrame, time_col: str, id_col: str) -> dict[date, list[str]]:
    """По строкам из новых файлов: какие сутки они задевают и какие ID там пришли.

    Пример результата: {2026-01-01: ["...-e00000020"]} - для поля «почему пересчитали этот день».
    """
    grouped = defaultdict(list)
    for row in df.select(F.to_date(time_col).alias("d"), id_col).distinct().collect():
        grouped[row["d"]].append(row[id_col])
    return {d: sorted(ids)[:MAX_IDS_IN_REASON] for d, ids in grouped.items()}


def compute_affected(spark, ch, meta: RunMeta, new_files: list[str]) -> tuple[list[date], list[dict]]:
    """Какие сутки пересчитать этому запуску и почему (правила - snapshot.affected_dates)."""
    # new_files - файлы архива, которых не было ни в одном прошлом опубликованном запуске
    new_events = spark_paths([k for k in new_files if k.startswith(RAW_EVENTS_PREFIX)])
    new_profiles = spark_paths([k for k in new_files if k.startswith(RAW_PROFILES_PREFIX)])
    event_dates = _dates_with_ids(read_events(spark, new_events), "event_time", "event_id")
    profile_dates = _dates_with_ids(read_profile_changes(spark, new_profiles), "effective_at", "change_id")
    # уже опубликованные дни (кроме этого же запуска) - их тоже нужно обновить, если новое их задело
    others = [r for r in clickhouse.published_runs(ch) if r["run_id"] != meta.run_id]
    return affected_dates(meta.interval_start.date(), meta.interval_end.date(), event_dates, profile_dates,
                          clickhouse.published_days(others))


def clean_profiles(spark, files: list[str]) -> DataFrame:
    """Изменения профилей из файлов среза: проверить и убрать повторы (результат - clean_profile_changes)."""
    valid, bad = validate_profile_changes(read_profile_changes(spark, spark_paths(files)))
    # в отличие от событий, плохой профиль не откладываем в сторону, а останавливаем расчёт:
    # без него SCD2 и все страны/тарифы в витринах были бы неверными
    if bad.count():
        raise ValueError(f"некорректные изменения профилей: {bad.limit(5).collect()}")
    return dedup_profile_changes(valid)


def run(**context) -> list[str]:
    """Записать clean_events (затронутые сутки), rejected_events, clean_profile_changes; вернуть сутки."""
    meta, ch = run_meta(context), clickhouse.client()
    snap = read_snapshot(meta.run_id)   # какие файлы архива читать (зафиксировал freeze_snapshot)
    with spark_session("prepare_data") as spark:
        # 1) какие сутки пересчитать
        days, reasons = compute_affected(spark, ch, meta, snap["new_files"])
        # 2) события: плохие строки -> rejected, хорошие -> без повторов
        valid, rejected = validate_events(read_events(spark, spark_paths(snap["files"]["events"])))
        # Витрины затронутых суток считаются по полному срезу, а не только по новым файлам.
        clean = add_meta(dedup_events(valid).filter(F.col("event_date").isin(days)), meta)
        # 3) сохранить всё в batch/<run_id>/ (повтор задачи перезапишет те же папки)
        write_partitioned(clean, dataset_path(meta.run_id, "clean_events"))
        add_meta(rejected, meta).write.mode("overwrite").parquet(dataset_path(meta.run_id, "rejected_events"))
        # профили - вся история (не только затронутые сутки): SCD2 нужна версия, действовавшая и до периода
        add_meta(clean_profiles(spark, snap["files"]["profile_changes"]), meta).write.mode("overwrite").parquet(
            dataset_path(meta.run_id, "clean_profile_changes"))
        stats = {"clean_events": clean.count(), "rejected_events": rejected.count()}
    # 4) записать в журнал запусков, какие сутки пересчитаны и почему
    clickhouse.record_run(ch, meta.run_id, affected_dates=days, affected_reasons=reasons)
    log_summary("Подготовка данных", {**stats, "affected_dates": [d.isoformat() for d in days]})
    return [d.isoformat() for d in days]   # уходит в XCom - следующие задачи берут сутки отсюда
