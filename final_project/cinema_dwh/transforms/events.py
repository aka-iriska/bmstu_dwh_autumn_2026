from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Проверка (валидация) и удаление повторов (дедупликация) для событий и изменений профилей.

# «Бизнес-поля» - всё, что описывает само событие; available_at и kafka_* у повторов могут отличаться
EVENT_FIELDS = ["event_id", "event_time", "user_id", "event_type", "content_id", "watch_time_seconds", "query"]
PROFILE_FIELDS = ["change_id", "user_id", "effective_at", "country", "subscription_tier"]
COUNTRIES = ["RU", "KZ", "BY", "AM"]        # допустимые значения по контракту данных
TIERS = ["basic", "standard", "premium"]


class DataConflictError(Exception):
    """Два сообщения с одним event_id / change_id, но разными бизнес-полями - данные испорчены, расчёт стоп."""


def _blank(column: str):
    """Условие Spark: в колонке null или одни пробелы."""
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def _event_reject_reason():
    """Колонка-выражение: почему событие плохое (текст первой найденной причины) или null, если хорошее."""
    is_view, is_search = F.col("event_type") == "view", F.col("event_type") == "search"
    return (   # when...when работает как if/elif: срабатывает первое подходящее условие
        F.when(_blank("event_id"), "missing event_id")
        .when(F.col("event_time").isNull(), "missing or bad event_time")   # null - и если время было кривым
        .when(F.col("user_id").isNull(), "missing user_id")
        .when(~F.col("event_type").isin("view", "search") | F.col("event_type").isNull(), "unknown event_type")
        # content_id = 0 валиден: проверяем только на null.
        .when(is_view & F.col("content_id").isNull(), "view without content_id")
        .when(is_view & (F.col("watch_time_seconds").isNull() | (F.col("watch_time_seconds") < 0)),
              "view without valid watch_time_seconds")
        .when(is_view & F.col("query").isNotNull(), "view with query")
        .when(is_search & _blank("query"), "search without query")
        .when(is_search & (F.col("content_id").isNotNull() | F.col("watch_time_seconds").isNotNull()),
              "search with view fields")
    )


def _profile_reject_reason():
    """Колонка-выражение: почему изменение профиля плохое или null, если хорошее."""
    missing = _blank("change_id") | F.col("user_id").isNull() | F.col("effective_at").isNull()
    return (
        F.when(missing, "missing required field")
        .when(~F.col("country").isin(COUNTRIES) | F.col("country").isNull(), "unknown country")
        .when(~F.col("subscription_tier").isin(TIERS) | F.col("subscription_tier").isNull(), "unknown tier")
    )


# плохие строки не теряются молча: уходят в rejected_events с причиной
def _split(df: DataFrame, reason) -> tuple[DataFrame, DataFrame]:
    """Разделить таблицу на две: (хорошие строки, плохие строки + колонка reject_reason с причиной)."""
    marked = df.withColumn("reject_reason", reason)
    valid = marked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    return valid, marked.filter(F.col("reject_reason").isNotNull())


def validate_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """События -> (корректные, отбракованные с причиной)."""
    return _split(df, _event_reject_reason())


def validate_profile_changes(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Изменения профилей -> (корректные, отбракованные с причиной)."""
    return _split(df, _profile_reject_reason())


def _dedup(df: DataFrame, key: str, fields: list[str]) -> DataFrame:
    """Оставить по одной строке на каждый ID (key); если копии одного ID различаются - DataConflictError."""
    # dropDuplicates по бизнес-полям: повторные доставки одинаковы, поэтому схлопываются в одну строку
    distinct = df.select(*fields).dropDuplicates()
    # если у ID после этого всё ещё >1 строки - значит копии отличались по содержанию
    conflicts = distinct.groupBy(key).count().filter("count > 1").limit(5).collect()
    if conflicts:
        raise DataConflictError(f"разные данные у одного {key}: {[row[key] for row in conflicts]}")
    return distinct


def dedup_events(df: DataFrame) -> DataFrame:
    """Таблица clean_events: события без повторов + колонка event_date (сутки события по UTC)."""
    return _dedup(df, "event_id", EVENT_FIELDS).withColumn("event_date", F.to_date("event_time"))


def dedup_profile_changes(df: DataFrame) -> DataFrame:
    """Таблица clean_profile_changes: изменения профилей без повторов."""
    return _dedup(df, "change_id", PROFILE_FIELDS)
