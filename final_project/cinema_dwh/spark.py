from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

from cinema_dwh.config import load_settings

# Два jar-файла, без которых Spark не умеет читать/писать s3a://...
S3_JARS = "/opt/airflow/s3-jars/hadoop-aws-3.3.4.jar,/opt/airflow/s3-jars/aws-java-sdk-bundle-1.12.262.jar"


def create_spark(app_name: str, master: str | None = None, with_s3: bool = True) -> SparkSession:
    """
    Запустить Spark внутри текущего контейнера и вернуть сессию (точка входа во все операции Spark).

    :param app_name: Имя для логов.
    :param master: Сколько ядер ("local[2]" = 2 потока на этой машине).
    :param with_s3: Настраивать ли s3 (нужно для тестов).
    """
    settings = load_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or settings.spark_master)       # local[...] - Spark без кластера, в этом же контейнере
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")   # все времена в проекте - UTC
        .config("spark.sql.shuffle.partitions", "4")   # данных мало: 200 частей по умолчанию - лишние накладные расходы
        .config("spark.ui.enabled", "false")           # веб-интерфейс Spark не нужен
        # ClickHouse понимает время в Parquet как TIMESTAMP_MICROS; формат по умолчанию (INT96) - нет
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # AQE: Spark сам дробит «тяжёлые» ключи при JOIN
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # при перезаписи данных с партициями заменять только те даты, что пишем сейчас
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if with_s3:
        builder = _with_s3a(builder, settings)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   # без этого лог задачи тонет в INFO-сообщениях Spark
    return spark


@contextmanager
def spark_session(app_name: str) -> Iterator[SparkSession]:
    """Spark на время одной задачи DAG: `with spark_session("имя") as spark: ...`, по выходу Spark останавливается."""
    spark = create_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()   # освобождает память JVM даже если задача упала


def _with_s3a(builder, settings):
    """Добавить настройки, чтобы Spark читал/писал пути s3a://cinema/... в нашем RustFS."""
    return (
        builder.config("spark.jars", S3_JARS)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)   # http://rustfs:9000
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")        # адреса вида rustfs:9000/cinema/...
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # внутри docker-сети без https
        # логин/пароль S3 берутся из переменных AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    )
