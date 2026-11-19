from pyspark.sql import functions as F

from common import (RESULTS, create_spark, interval, log_summary, read_campaigns, run_context, utc_ts, with_meta,
                    write_partition)

TABLE = f"{RESULTS}/campaign_recipients"


def recipients(scd2, campaigns):
    """
    JOIN из ДЗ 2.2.

    По условию строка попадает в результат, если одновременно:
    * домен user_email версии (часть после @) равен email_domain рассылки, сравнение в нижнем регистре;
    * valid_from <= send_at;
    * send_at < valid_to либо valid_to IS NULL.
    Берём все версии, а не только is_current: нужны атрибуты на момент рассылки.
    СТРАТЕГИЯ: рассылок за сутки ЕДИНИЦЫ, поэтому они идут в broadcast,
    и снимок SCD2 не перемешивается (shuffle нет) - вывод из ДЗ 2.2."""

    users = scd2.withColumn("domain", F.lower(F.substring_index("user_email", "@", -1)))
    c = F.broadcast(campaigns)
    cond = ((users.domain == F.lower(c.email_domain)) & (users.valid_from <= c.send_at)
            & (users.valid_to.isNull() | (c.send_at < users.valid_to)))
    # По условию: поля рассылки и профиля в исходном виде (регистр не меняем), user_valid_* — из версии SCD2
    return users.join(c, cond).select(
        "campaign_id", "campaign_name", "send_at", "email_domain",
        "user_id", "user_fio", "user_email", "user_login",
        F.col("valid_from").alias("user_valid_from"), F.col("valid_to").alias("user_valid_to"))


def run(meta, scd2_path):
    """Задача 2: получатели рассылок за D по снимку user_scd2 за тот же D (исторический JOIN)."""
    d = meta["start"].date().isoformat()

    # По условию вторая задача читает результат первой именно за тот же D, а не последний доступный снимок
    assert scd2_path.endswith(f"snapshot_date={d}"), f"снимок {scd2_path} не за {d}"

    path = f"{TABLE}/calculation_date={d}"   # по условию партиция calculation_date=D
    spark = create_spark(f"campaign_recipients_{d}")

    try:
        scd2 = spark.read.parquet(scd2_path)
        # рассылки с data_interval_start <= send_at < data_interval_end
        campaigns = read_campaigns(spark).filter(
            (F.col("send_at") >= utc_ts(meta["start"])) & (F.col("send_at") < utc_ts(meta["end"])))
        result = with_meta(recipients(scd2, campaigns), meta)
        result.explain()   # по условию нужен план выполнения JOIN -> печатается в лог задачи

        # Нет рассылок/получателей -> задача завершается успешно, пишется пустой Parquet, старые строки удаляются
        write_partition(result, path)

        # Сводка по записанному результату -> в лог задачи; нулевые количества тоже видны
        written = spark.read.parquet(path)
        per_campaign = {r.campaign_id: r["count"] for r in written.groupBy("campaign_id").count().collect()}
        day = campaigns.orderBy("send_at").collect()   # рассылок за сутки единицы
        summary = {"интервал": interval(meta), "run_id": meta["run_id"], "снимок SCD2": scd2_path,
                   "рассылок": len(day)}
        summary.update({f"  {c.campaign_id} {c.email_domain} в {c.send_at:%H:%M}": per_campaign.get(c.campaign_id, 0)
                        for c in day})
        summary.update({"строк (получателей)": written.count(), "путь": path})
        log_summary(f"campaign_recipients: витрина за {d}", summary)
    finally:
        spark.stop()
    return path


def main(scd2_path, **ctx):
    return run(run_context(ctx), scd2_path)
