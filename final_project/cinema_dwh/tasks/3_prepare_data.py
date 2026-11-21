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

# Шаг 3 DAG: читает файлы архива из среза, отбраковывает плохие строки, убирает повторы,
# решает, какие сутки пересчитать, и пишет очищенные данные в batch/<run_id>/.

MAX_IDS_IN_REASON = 10   # причины в batch_runs читают люди - длинные списки ID не нужны


def _dates_with_ids(df: DataFrame, time_col: str, id_col: str) -> dict[date, list[str]]:
    """По строкам из новых файлов: какие сутки они задевают и какие ID там пришли.

    Пример результата: {2026-01-01: ["...-e00000020"]} - для поля «почему пересчитали этот день».
    """
    day_and_id = df.select(F.to_date(time_col).alias("d"), id_col).distinct().collect()
    grouped = defaultdict(list)
    for row in day_and_id:
        grouped[row["d"]].append(row[id_col])
    return {d: sorted(ids)[:MAX_IDS_IN_REASON] for d, ids in grouped.items()}


def compute_affected(spark, ch, meta: RunMeta, new_files: list[str]) -> tuple[list[date], list[dict]]:
    """Какие сутки пересчитать этому запуску и почему (правила - snapshot.affected_dates)."""
    # new_files - файлы архива, которых не было ни в одном прошлом опубликованном запуске
    new_event_keys = [k for k in new_files if k.startswith(RAW_EVENTS_PREFIX)]
    new_profile_keys = [k for k in new_files if k.startswith(RAW_PROFILES_PREFIX)]
    new_events = read_events(spark=spark, paths=spark_paths(keys=new_event_keys))
    new_profiles = read_profile_changes(spark=spark, paths=spark_paths(keys=new_profile_keys))
    event_dates = _dates_with_ids(df=new_events, time_col="event_time", id_col="event_id")
    profile_dates = _dates_with_ids(df=new_profiles, time_col="effective_at", id_col="change_id")
    # уже опубликованные дни (кроме этого же запуска) - их тоже нужно обновить, если новое их задело
    all_published = clickhouse.published_runs(ch=ch)
    others = [r for r in all_published if r["run_id"] != meta.run_id]
    return affected_dates(interval_start=meta.interval_start.date(),
                          interval_end=meta.interval_end.date(),
                          event_dates=event_dates,
                          profile_effective_dates=profile_dates,
                          published_dates=clickhouse.published_days(runs=others))


def clean_profiles(spark, files: list[str]) -> DataFrame:
    """Изменения профилей из файлов среза: проверить и убрать повторы (результат - clean_profile_changes)."""
    raw = read_profile_changes(spark=spark, paths=spark_paths(keys=files))
    valid, bad = validate_profile_changes(df=raw)
    # в отличие от событий, плохой профиль не откладываем в сторону, а останавливаем расчёт:
    # без него SCD2 и все страны/тарифы в витринах были бы неверными
    if bad.count():
        raise ValueError(f"некорректные изменения профилей: {bad.limit(5).collect()}")
    return dedup_profile_changes(df=valid)


def run(**context) -> list[str]:
    """Записать clean_events (затронутые сутки), rejected_events, clean_profile_changes; вернуть сутки."""
    meta = run_meta(context=context)
    ch = clickhouse.client()
    snap = read_snapshot(run_id=meta.run_id)   # какие файлы архива читать (зафиксировал freeze_snapshot)
    with spark_session(app_name="prepare_data") as spark:
        # 1) какие сутки пересчитать
        days, reasons = compute_affected(spark=spark, ch=ch, meta=meta, new_files=snap["new_files"])
        # 2) события: плохие строки -> rejected, хорошие -> без повторов
        raw_events = read_events(spark=spark, paths=spark_paths(keys=snap["files"]["events"]))
        valid, rejected = validate_events(df=raw_events)
        unique_events = dedup_events(df=valid)
        # Витрины затронутых суток считаются по полному срезу, а не только по новым файлам.
        affected_events = unique_events.filter(F.col("event_date").isin(days))
        clean = add_meta(df=affected_events, meta=meta)
        # 3) сохранить всё в batch/<run_id>/ (повтор задачи перезапишет те же папки)
        write_partitioned(df=clean, path=dataset_path(run_id=meta.run_id, name="clean_events"))
        rejected_with_meta = add_meta(df=rejected, meta=meta)
        rejected_with_meta.write.mode("overwrite").parquet(dataset_path(run_id=meta.run_id, name="rejected_events"))
        # профили - вся история (не только затронутые сутки): SCD2 нужна версия, действовавшая и до периода
        profiles = clean_profiles(spark=spark, files=snap["files"]["profile_changes"])
        profiles_with_meta = add_meta(df=profiles, meta=meta)
        profiles_with_meta.write.mode("overwrite").parquet(dataset_path(run_id=meta.run_id, name="clean_profile_changes"))
        stats = {"clean_events": clean.count(), "rejected_events": rejected.count()}
    # 4) записать в журнал запусков, какие сутки пересчитаны и почему
    clickhouse.record_run(ch=ch, run_id=meta.run_id, affected_dates=days, affected_reasons=reasons)
    day_strings = [d.isoformat() for d in days]
    log_summary(title="Подготовка данных", values={**stats, "affected_dates": day_strings})
    return day_strings   # уходит в XCom - следующие задачи берут сутки отсюда
