import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# hw2-1 создаётся в docker-compose.yml
SOURCE = "s3a://hw2-1/events/"       # исходная таблица из event_log/
TARGET = "s3a://hw2-1/users_scd2/"   # готовая SCD2 таблица

# Колонки профиля: новая версия появляется, только если поменялась одна из них
ATTRIBUTES = ["user_fio", "user_email", "user_login"]
SCHEMA = "event_time TIMESTAMP, user_id BIGINT, user_fio STRING, user_email STRING, user_login STRING"


def create_spark() -> SparkSession:
    """Сессия Spark с доступом к S3 (RustFS).

    Значения S3_ENDPOINT, S3_ACCESS_KEY, SPARK_MASTER и др. приходят из окружения контейнера:
    они прописаны в docker-compose.yml, в разделе environment сервиса jupyter.
    """
    return (
        SparkSession.builder.appName("hw2_1_scd2")
        # куда отправлять вычисления: мастер Spark-кластера из docker-compose
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # адрес драйвера (контейнер jupyter), по которому к нему подключается воркер
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "localhost"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        # берём 2 ядра из 4, чтобы скрипт и ноутбук могли работать одновременно
        .config("spark.cores.max", "2")
        # все даты и время считаем в UTC
        .config("spark.sql.session.timeZone", "UTC")
        # адрес S3 (RustFS) и логин/пароль к нему
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        # бакет указывается в пути (http://rustfs:9000/hw2-1/...), а не в имени сервера; без https
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def build_scd2(events):
    """Строит версии профиля (SCD2) из лога событий."""

    # группируем события по пользователю в оконной функции
    by_time = Window.partitionBy("user_id").orderBy("event_time")

    prev_event_time = F.lag("event_time").over(by_time)
    # У первого события пользователя предыдущего нет, тогда lag возвращает NULL
    is_first_event = prev_event_time.isNull()

    prev_fio = F.lag("user_fio").over(by_time)
    prev_email = F.lag("user_email").over(by_time)
    prev_login = F.lag("user_login").over(by_time)


    # Изменилось ли поле по сравнению с предыдущим событием
    fio_changed = ~F.col("user_fio").eqNullSafe(prev_fio)
    email_changed = ~F.col("user_email").eqNullSafe(prev_email)
    login_changed = ~F.col("user_login").eqNullSafe(prev_login)

    # Новая версия начинается, если изменилось хоть одно поле профиля
    is_new_version = is_first_event | fio_changed | email_changed | login_changed

    versions = (
        events.withColumn(
            colName = "is_new_version", col = is_new_version
        )
        .filter("is_new_version")
        .select("user_id", *ATTRIBUTES, F.col("event_time").alias("valid_from"))
    )

    # Версия действует до начала следующей версии того же пользователя (lead).
    by_version = Window.partitionBy("user_id").orderBy("valid_from")

    return (versions
        .withColumn(
            # берём значение valid_from из следующей строки в окне
            colName = "valid_to", col = F.lead("valid_from").over(by_version)
        )
        .withColumn(
            colName="is_current", col = F.col("valid_to").isNull()
        )
    )


def main() -> None:
    spark = create_spark()

    # Читаем все CSV из папки SOURCE как одну таблицу.
    events = (
        spark.read.schema(SCHEMA)
        .option("header", True)                                # первая строка файла — названия колонок
        .option("escape", '"')                                 # кавычки внутри значений удвоены, как пишет генератор
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssXXX")  # формат времени: 2026-09-01T00:00:03Z
        .csv(SOURCE)
    )

    scd2 = build_scd2(events)

    # Записываем результат в S3 одним Parquet-файлом
    (scd2.orderBy("user_id", "valid_from").coalesce(1).write
     .mode("overwrite") # стирает старый результат и пишет новый, поэтому повторный запуск на тех же данных не создаёт дублей
     .parquet(TARGET))

    print(f"Готово: {spark.read.parquet(TARGET).count()} строк в {TARGET}")
    spark.stop()


if __name__ == "__main__":
    main()
