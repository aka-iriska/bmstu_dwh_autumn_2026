from __future__ import annotations

RAW_EVENTS_PREFIX = "raw/events/"
RAW_PROFILES_PREFIX = "raw/profile_changes/"
CATALOGUE_KEY = "raw/catalogue/catalogue.jsonl"

# Витрины: имя папки в S3 = имя таблицы ClickHouse без префикса batch_.
MARTS = ("views_minute", "views_country_tier_daily", "top_content_daily", "search_words_daily")


# все пути строятся из run_id, поэтому между задачами DAG их не передают
def run_dir(run_id: str) -> str:
    """Папка запуска: batch/<run_id>/."""
    return f"batch/{run_id}/"


def snapshot_key(run_id: str) -> str:
    """Срез запуска (только имена файлов)."""
    return f"{run_dir(run_id)}snapshot.json"


def dataset_key(run_id: str, name: str) -> str:
    """Parquet-набор запуска: clean_events, rejected_events, user_profile_scd2 и т. п."""
    return f"{run_dir(run_id)}{name}/"


def mart_key(run_id: str, mart: str) -> str:
    """Папка витрины запуска."""
    if mart not in MARTS:
        raise ValueError(f"неизвестная витрина: {mart}")
    return f"{run_dir(run_id)}marts/{mart}/"


def checks_key(run_id: str, name: str) -> str:
    """Отчёт проверки запуска: rdd_vs_df или quality."""
    return f"{run_dir(run_id)}checks/{name}.json"


def s3a(key: str, bucket: str) -> str:
    """Полный путь для Spark."""
    return f"s3a://{bucket}/{key}"
