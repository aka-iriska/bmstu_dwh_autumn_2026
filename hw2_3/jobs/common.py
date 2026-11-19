import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ.get("S3_BUCKET", "hw23")
# На готовом наборе data/demo (6 000 событий, 100 пользователей, seed=42, 1 января – 1 февраля 2026).
DATASET = os.environ.get("HW23_DATASET", "demo")
RAW = f"s3a://{BUCKET}/raw/{DATASET}"            # входы: raw/demo/events/*.csv, raw/demo/campaigns.csv
RESULTS = f"s3a://{BUCKET}/results/{DATASET}"    # по условию результаты в raw/ не пишем
TS = "yyyy-MM-dd'T'HH:mm:ssXXX"   # 2026-01-01T00:10:55Z

# По условию (раздел 5) должно быть шесть обязательных метаполей в обеих таблицах
META = ["_dag_id", "_run_id", "_task_id", "_data_interval_start", "_data_interval_end", "_loaded_at"]


def create_spark(name):
    """Настройки S3A взяты из scripts/check_environment.py комплекта."""

    jars = "/opt/airflow/s3-jars/"
    spark = (SparkSession.builder.appName(name)
             .master(os.environ.get("SPARK_MASTER", "local[2]"))
             .config("spark.driver.memory", "1g")
             # по условию все времена трактуются как UTC, README просит явно задать UTC в расчёте
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .config("spark.jars", f"{jars}hadoop-aws-3.3.4.jar,{jars}aws-java-sdk-bundle-1.12.262.jar")
             .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])   # http://rustfs:9000 внутри Docker
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark, raw=RAW):
    # По условию: event_time, user_id, user_fio, user_email, user_login; user_id — bigint (вмещает uint32 из ДЗ 2.1).
    return (spark.read.schema("event_time TIMESTAMP, user_id BIGINT, user_fio STRING, user_email STRING, user_login STRING")
            .option("header", True)
            .option("escape", '"')
            .option("timestampFormat", TS)
            .csv(f"{raw}/events/*.csv"))


def read_campaigns(spark, raw=RAW):
    # По условию: campaign_id, email_domain, send_at, campaign_name; campaign_id — bigint и уникален
    return (spark.read.schema("campaign_id BIGINT, email_domain STRING, send_at TIMESTAMP, campaign_name STRING")
            .option("header", True)
            .option("timestampFormat", TS)
            .csv(f"{raw}/campaigns.csv"))


def run_context(ctx):
    """По условию контекст запуска берём средствами Airflow: dag_id, run_id, task_id и границы интервала."""

    return {"dag_id": ctx["dag"].dag_id,
            "run_id": ctx["run_id"],
            "task_id": ctx["task"].task_id,
            "start": ctx["data_interval_start"],
            "end": ctx["data_interval_end"]}


def with_meta(df, meta):
    """
    Добавляет шесть метаполей.

    По условию _loaded_at фиксируется один раз на попытку задачи,
    поэтому это одно значение-литерал, а не current_timestamp() на каждую строку.

    Метаполя добавляются после SCD2, поэтому в сравнение атрибутов они не попадают (тоже по условию)."""

    loaded_at = datetime.now(timezone.utc)
    return df.select("*",
                     F.lit(meta["dag_id"]).alias("_dag_id"),
                     F.lit(meta["run_id"]).alias("_run_id"),
                     F.lit(meta["task_id"]).alias("_task_id"),
                     utc_ts(meta["start"]).alias("_data_interval_start"),
                     utc_ts(meta["end"]).alias("_data_interval_end"),
                     utc_ts(loaded_at).alias("_loaded_at"))


def utc_ts(dt):
    """aware datetime -> timestamp-литерал Spark (строка в UTC, сессия тоже в UTC)."""

    return F.lit(dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")).cast("timestamp")


def write_partition(df, path):
    """
    Пишет результат в каталог одной даты (…/snapshot_date=D/ или …/calculation_date=D/).

    По условию повтор за тот же D заменяет только эту дату и не добавляет дублей -> overwrite по пути партиции.
    Пустой результат тоже сохраняется как читаемый Parquet с полной схемой (Spark пишет файл со схемой).
    """

    (df.coalesce(1) # сливает результат в один файл, иначе получилось бы несколько мелких
     .write.mode("overwrite").parquet(path))


log = logging.getLogger("airflow.task")   # логгер задачи Airflow: строки попадают в лог попытки с временем и уровнем


def log_summary(title, values):
    """Сводка результата в лог задачи одним блоком: «название : значение» в столбик."""
    width = max(len(k) for k in values)
    log.info("\n".join([title] + [f"    {k.ljust(width)} : {v}" for k, v in values.items()]))


def interval(meta):
    """[2026-01-23 00:00, 2026-01-24 00:00) UTC — интервал запуска для логов."""
    fmt = lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return f"[{fmt(meta['start'])}, {fmt(meta['end'])}) UTC"
