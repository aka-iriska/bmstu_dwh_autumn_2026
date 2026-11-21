from __future__ import annotations

import json
from functools import lru_cache

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cinema_dwh.config import load_settings


@lru_cache(maxsize=1)   # один клиент на процесс
def client():
    """Клиент boto3 для нашего S3 (RustFS в docker-compose, адрес из S3_ENDPOINT)."""
    settings = load_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,   # http://rustfs:9000,
        region_name="us-east-1",             # RustFS регион не важен, но boto3 требует какой-то
        # адрес вида http://rustfs:9000/cinema/<ключ>, а не http://cinema.rustfs:9000/<ключ>
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def _bucket() -> str:
    """Имя бакета проекта (cinema) из переменной S3_BUCKET."""
    return load_settings().s3_bucket


def list_keys(prefix: str) -> list[str]:
    """Пути всех файлов в бакете, которые начинаются с prefix («содержимое папки» prefix).

    Пример: list_keys("raw/events/") -> ["raw/events/ingest_date=.../part-p0-o0-24.jsonl", ...].
    Список отсортирован: так срез запуска одинаков при каждом вызове.
    """
    keys = []
    # S3 отдаёт список страницами по 1000 файлов
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=_bucket(), Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))   # "Contents" нет у пустой страницы
    return sorted(keys)


def exists(key: str) -> bool:
    """Лежит ли в бакете файл с таким путём (используется для snapshot.json запуска)."""
    try:
        client().head_object(Bucket=_bucket(), Key=key)   # запрос только метаданных, без скачивания файла
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):   # файла нет
            return False
        raise   # любая другая ошибка (нет доступа, сеть) - настоящая проблема, не «файла нет»


# Здесь только служебные JSON-файлы (snapshot.json, отчёты checks/*.json);
# сами данные (JSONL, Parquet) читает и пишет Spark.
def read_json(key: str):
    """Скачать JSON-файл по пути key и вернуть его содержимое как dict/list."""
    body = client().get_object(Bucket=_bucket(), Key=key)["Body"].read()
    return json.loads(body)


def write_json(key: str, obj) -> None:
    """Сохранить obj в бакет JSON-файлом по пути key (перезаписывает, если файл уже есть)."""
    # ensure_ascii=False - кириллица остаётся читаемой; indent=2 - читаемый формат;
    # default=str - даты и прочие не-JSON типы превращаются в строки
    body = json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    client().put_object(Bucket=_bucket(), Key=key, Body=body, ContentType="application/json")
