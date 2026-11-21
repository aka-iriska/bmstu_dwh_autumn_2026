import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from cinema_dwh import clickhouse as chm
from cinema_dwh import s3io
from cinema_dwh.config import load_settings
from tests.helpers import D

# нужен поднятый стенд: docker compose run --rm tools pytest -q -m integration
pytestmark = pytest.mark.integration
TABLE = "batch_search_words_daily"
PREFIX = "tests/publish/search_words_daily/"


@pytest.fixture
def ch():
    client = chm.client()
    client.command(f"TRUNCATE TABLE cinema.{TABLE}")
    yield client
    client.command(f"TRUNCATE TABLE cinema.{TABLE}")


def upload_mart(spark, rows) -> int:
    """Записать витрину слов в S3 как Parquet (Spark локально -> boto3) и вернуть число строк."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows, "event_date date, word string, word_count long")
    now = datetime(2026, 11, 21, tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df = df.select("*", F.lit("btest").alias("run_id"), F.lit("2026-01-01 00:00:00").cast("timestamp").alias("interval_start"),
                   F.lit("2026-01-02 00:00:00").cast("timestamp").alias("interval_end"),
                   F.lit(now).cast("timestamp").alias("processed_at"))
    with tempfile.TemporaryDirectory() as tmp:
        df.coalesce(1).write.mode("overwrite").parquet(tmp + "/out")
        for old in s3io.list_keys(PREFIX):
            s3io.client().delete_object(Bucket=load_settings().s3_bucket, Key=old)
        for part in Path(tmp, "out").glob("*.parquet"):
            s3io.client().upload_file(str(part), load_settings().s3_bucket, PREFIX + part.name)
    return df.count()


def table_rows(ch):
    return ch.query(f"SELECT event_date, word, word_count FROM cinema.{TABLE} ORDER BY event_date, word").result_rows


def test_publish_twice_no_duplicates(ch, spark):
    n = upload_mart(spark, [(D(1), "комедия", 2), (D(2), "драма", 1)])
    chm.publish_mart(ch, TABLE, PREFIX + "*.parquet", [D(1), D(2)], n)
    chm.publish_mart(ch, TABLE, PREFIX + "*.parquet", [D(1), D(2)], n)
    assert table_rows(ch) == [(D(1), "комедия", 2), (D(2), "драма", 1)]


def test_emptied_day_is_dropped_and_leftover_staging_ignored(ch, spark):
    n = upload_mart(spark, [(D(1), "комедия", 2), (D(2), "драма", 1)])
    chm.publish_mart(ch, TABLE, PREFIX + "*.parquet", [D(1), D(2)], n)
    ch.command(f"CREATE TABLE IF NOT EXISTS cinema.stg_{TABLE} AS cinema.{TABLE}")   # «остаток упавшего запуска»
    n = upload_mart(spark, [(D(1), "комедия", 3)])
    chm.publish_mart(ch, TABLE, PREFIX + "*.parquet", [D(1), D(2)], n)
    assert table_rows(ch) == [(D(1), "комедия", 3)]


def test_wrong_row_count_stops_publish(ch, spark):
    n = upload_mart(spark, [(D(1), "комедия", 2)])
    with pytest.raises(chm.PublishError):
        chm.publish_mart(ch, TABLE, PREFIX + "*.parquet", [D(1)], n + 1)
    assert table_rows(ch) == []


def test_record_run_merges_fields():
    ch = chm.client()
    run_id = "btest_" + datetime.now(timezone.utc).strftime("%H%M%S%f")
    chm.record_run(ch, run_id, airflow_run_id="manual__x", status="running")
    chm.record_run(ch, run_id, affected_dates=[D(1), D(2)], affected_reasons=[{"date": "2026-01-01"}])
    row = chm.latest_run(ch, run_id)
    assert row["airflow_run_id"] == "manual__x" and row["status"] == "running"
    assert row["affected_dates"] == [D(1), D(2)]
    ch.command("ALTER TABLE cinema.batch_runs DELETE WHERE run_id = {rid:String}", parameters={"rid": run_id})
