from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"  # формат времени в данных: 2026-01-01T00:00:00Z (X - это «Z», то есть UTC)

# Схемы - «какие колонки и каких типов» у трёх входных файлов (поля - data/README.md).
# Задаём их явно, а не даём Spark угадывать: content_id = 0 и null не путаются,
# а лишние поля из архива (kafka_topic, kafka_offset, archived_at...) просто не читаются.

# Одна строка events.jsonl: просмотр (view) или поиск (search)
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("user_id", LongType()),
    StructField("event_type", StringType()),
    StructField("content_id", LongType()),           # у поиска null
    StructField("watch_time_seconds", LongType()),   # у поиска null
    StructField("query", StringType()),              # у просмотра null
    StructField("available_at", StringType()),       # когда сообщение пришло - для расчётов не нужно
])

# Одна строка profile_changes.jsonl: полное состояние профиля с момента effective_at
PROFILE_SCHEMA = StructType([
    StructField("change_id", StringType()),
    StructField("user_id", LongType()),
    StructField("effective_at", StringType()),
    StructField("country", StringType()),
    StructField("subscription_tier", StringType()),
    StructField("available_at", StringType()),
])

# Одна строка catalogue.jsonl: фильм или сериал
CATALOGUE_SCHEMA = StructType([
    StructField("content_id", LongType()),
    StructField("category", StringType()),
    StructField("duration_seconds", LongType()),
])


def _to_ts(column: str):
    """Колонку-строку вида 2026-01-01T00:00:00Z -> timestamp; кривая строка -> null (её потом отбракует валидация)."""
    return F.to_timestamp(F.col(column), TS_FORMAT)


def _read_jsonl(spark: SparkSession, schema: StructType, paths: list[str]) -> DataFrame:
    """Прочитать JSONL-файлы (одна строка = один JSON) по схеме schema в один DataFrame."""
    if not paths:
        # spark.read.json([]) падает - для пустого списка отдаём пустую таблицу с теми же колонками
        return spark.createDataFrame([], schema)
    return spark.read.schema(schema).json(paths)


def read_events(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Таблица событий из файлов paths (events.jsonl набора или файлы архива raw/events/)."""
    return _read_jsonl(spark, EVENT_SCHEMA, paths).withColumn("event_time", _to_ts("event_time"))


def read_profile_changes(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Таблица изменений профилей из файлов paths (profile_changes.jsonl или raw/profile_changes/)."""
    return _read_jsonl(spark, PROFILE_SCHEMA, paths).withColumn("effective_at", _to_ts("effective_at"))


def read_catalogue(spark: SparkSession, path: str) -> DataFrame:
    """Таблица каталога (content_id, category, duration_seconds) из одного файла catalogue.jsonl."""
    return spark.read.schema(CATALOGUE_SCHEMA).json(path)
