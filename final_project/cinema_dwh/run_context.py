from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from cinema_dwh.config import BATCH_END_EXCLUSIVE, BATCH_START


META_COLUMNS = ("run_id", "interval_start", "interval_end", "processed_at")


@dataclass(frozen=True)
class RunMeta:
    """
    Метаполя запуска.

    :cvar run_id: какой запуск
    :cvar interval_start: Начало запрашиваемого периода.
    :cvar interval_end: Конец запрашиваемого периода.
    :cvar processed_at: Дата прочтения.
    """

    run_id: str
    interval_start: datetime
    interval_end: datetime
    processed_at: datetime


def make_run_id(interval_start: datetime, interval_end: datetime, airflow_run_id: str) -> str:
    """Имя запуска вида b20260101_8393d69f - им же названа папка batch/<run_id>/ в S3.

    b + дата начала интервала + 8 символов хеша. Повтор того же запуска Airflow даёт то же имя
    (та же папка, тот же срез). В хеш входит и интервал: тот же запуск Airflow с другим периодом
    в параметрах получит новое имя, а значит - свой срез, а не старый.
    """
    key = f"{airflow_run_id}|{to_iso(interval_start)}|{to_iso(interval_end)}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:8]
    return f"b{interval_start.strftime('%Y%m%d')}_{digest}"


def _is_utc_midnight(moment: datetime) -> bool:
    """Время ровно 00:00:00 по UTC."""
    return moment.astimezone(timezone.utc).time() == time(0, 0)


def validate_interval(start: datetime, end: datetime) -> None:
    """Проверить расчётный интервал: целые сутки UTC внутри января 2026; иначе ValueError (задача падает)."""
    if not (_is_utc_midnight(start) and _is_utc_midnight(end)):   # витрины подменяются по суткам целиком
        raise ValueError(f"границы интервала должны быть полночью UTC: {start} - {end}")
    if start >= end:
        raise ValueError(f"начало интервала должно быть раньше конца: {start} - {end}")
    if start.date() < BATCH_START or end.date() > BATCH_END_EXCLUSIVE:   # данные наборов - только январь 2026
        raise ValueError(f"интервал вне [{BATCH_START}, {BATCH_END_EXCLUSIVE}): {start} - {end}")


def interval_days(start: datetime, end: datetime) -> list[date]:
    """Сутки интервала [start, end): 1 янв 00:00 - 3 янв 00:00 -> [1 янв, 2 янв] (конец не включается)."""
    first, last = start.date(), end.date()
    return [first + timedelta(days=i) for i in range((last - first).days)]


def to_iso(moment: datetime) -> str:
    """datetime -> строка как в данных: 2026-01-01T00:00:00Z (так время лежит в XCom и snapshot.json)."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def from_iso(text: str) -> datetime:
    """Строка 2026-01-01T00:00:00Z (или с +00:00) -> datetime в UTC; обратное к to_iso."""
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def add_meta(df, meta: RunMeta):
    """Добавить к таблице Spark 4 колонки метаполей с одинаковым значением во всех строках."""
    from pyspark.sql import functions as F   # pyspark нужен только задачам Spark - не грузим его без нужды

    def ts(moment: datetime):
        """Константа-время для Spark (сессия Spark в UTC)."""
        return F.lit(moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    return df.select("*", F.lit(meta.run_id).alias("run_id"), ts(meta.interval_start).alias("interval_start"),
                     ts(meta.interval_end).alias("interval_end"), ts(meta.processed_at).alias("processed_at"))


def drop_meta(df):
    """Убрать 4 колонки метаполей - когда следующий шаг читает результат предыдущего и добавит свои."""
    return df.drop(*META_COLUMNS)
