from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone

# Начало истории: от этих суток строится цепочка опубликованных дней для границы T.
BATCH_START = date(2026, 1, 1)
# Готовые наборы - январь 2026; интервалы вне [BATCH_START, BATCH_END_EXCLUSIVE) отклоняются.
BATCH_END_EXCLUSIVE = date(2026, 2, 1)
# valid_to у текущей версии профиля в SCD2.
FAR_FUTURE = datetime(2100, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Settings:
    """Адреса и доступы к S3, ClickHouse, Kafka и режим Spark."""

    s3_endpoint: str
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    kafka_bootstrap: str
    spark_master: str


# переменные окружения задаёт docker-compose.yaml (якорь x-project-env)
def load_settings() -> Settings:
    """Собрать настройки из окружения (значения по умолчанию совпадают с docker-compose)."""
    env = os.environ.get
    return Settings(
        s3_endpoint=env("S3_ENDPOINT", "http://rustfs:9000"),
        s3_bucket=env("S3_BUCKET", "cinema"),
        s3_access_key=env("AWS_ACCESS_KEY_ID", ""),
        s3_secret_key=env("AWS_SECRET_ACCESS_KEY", ""),
        clickhouse_host=env("CLICKHOUSE_HOST", "clickhouse"),
        clickhouse_port=int(env("CLICKHOUSE_PORT", "8123")),
        clickhouse_user=env("CLICKHOUSE_USER", "cinema"),
        clickhouse_password=env("CLICKHOUSE_PASSWORD", ""),
        kafka_bootstrap=env("KAFKA_BOOTSTRAP", "kafka:9092"),
        spark_master=env("SPARK_MASTER", "local[2]"),
    )
