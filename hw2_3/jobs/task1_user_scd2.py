from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (RESULTS, create_spark, interval, log_summary, read_events, run_context, utc_ts, with_meta,
                    write_partition)

# По условию новая версия появляется только при изменении ФИО, почты или логина
ATTRIBUTES = ["user_fio", "user_email", "user_login"]
TABLE = f"{RESULTS}/user_scd2"


def build_scd2(events):
    """
    SCD2 из ДЗ 2.1.

    Правила по условию:
    * события упорядочены по event_time (порядок строк в файлах не гарантирован -> сортируем окном);
    * у последней версии valid_to = NULL и is_current = true, интервалы [valid_from, valid_to) не пересекаются.
    """

    by_time = Window.partitionBy("user_id").orderBy("event_time")
    changed = F.lag("event_time").over(by_time).isNull()          # первое событие пользователя
    for c in ATTRIBUTES:
        changed = changed | ~F.col(c).eqNullSafe(F.lag(c).over(by_time))

    by_version = Window.partitionBy("user_id").orderBy("valid_from")
    return (events.withColumn("is_new_version", changed).filter("is_new_version")
            .select("user_id", *ATTRIBUTES, F.col("event_time").alias("valid_from"))   # исходные значения, без lower()
            .withColumn("valid_to", F.lead("valid_from").over(by_version))
            .withColumn("is_current", F.col("valid_to").isNull()))   # не закрываем границей суток


def run(meta):
    """
    Задача 1: снимок SCD2 всей истории пользователей по событиям event_time < data_interval_end.

    Пересобирает снимок за D = дату начала интервала (по условию) и возвращает путь к нему.
    """

    d = meta["start"].date().isoformat() # дата начала интервала в UTC
    # По условию каждый снимок — отдельная партиция user_scd2/snapshot_date=D/
    path = f"{TABLE}/snapshot_date={d}"

    spark = create_spark(f"user_scd2_{d}")
    try:
        events = read_events(spark).filter(F.col("event_time") < utc_ts(meta["end"])) # вся история с event_time < data_interval_end
        snapshot = with_meta(build_scd2(events), meta)
        write_partition(snapshot, path)   # допускается полностью пересобирать снимок из исходных событий

        # Сводка по записанному результату (читаем из S3, а не из плана) — в лог задачи
        stats = spark.read.parquet(path).agg(
            F.count("*").alias("rows"), F.countDistinct("user_id").alias("users"),
            F.sum(F.col("is_current").cast("int")).alias("current")).first()

        log_summary(f"user_scd2: снимок за {d}", {
            "интервал": interval(meta),
            "run_id": meta["run_id"],
            "строк (версий)": stats.rows,
            "пользователей": stats.users,
            "текущих версий": stats.current or 0,   # у пустого снимка sum() = NULL
            "путь": path,
        })
    finally:
        spark.stop()
    return path


def main(**ctx):
    """
    Контекст запуска приходит из Airflow.
    Возвращает только путь: по условию содержимое таблиц через XCom не передаём."""
    return run(run_context(ctx))
