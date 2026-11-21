from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from cinema_dwh.config import FAR_FUTURE

SCD2_COLUMNS = ["user_id",
                "country",
                "subscription_tier",
                "valid_from",
                "valid_to",
                "is_current",
                "version",
                "source_change_id"]


def _drop_repeated_states(changes: DataFrame) -> DataFrame:
    """Убрать изменения, повторяющие предыдущее состояние пользователя: они не создают версию."""
    by_time = Window.partitionBy("user_id").orderBy("effective_at")
    state = F.struct("country", "subscription_tier")
    with_previous = changes.withColumn("_previous", F.lag(state).over(by_time))
    keep = F.col("_previous").isNull() | (F.col("_previous") != state)
    return with_previous.filter(keep).drop("_previous")


# история строится заново по всем изменениям среза - позднее изменение само встаёт между известными версиями
def build_scd2(changes: DataFrame) -> DataFrame:
    """Версии профилей из очищенных изменений (clean_profile_changes)."""
    far_future = F.lit(FAR_FUTURE.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    by_time = Window.partitionBy("user_id").orderBy("valid_from")
    versions = (
        _drop_repeated_states(changes)
        .withColumnRenamed("effective_at", "valid_from")
        .withColumnRenamed("change_id", "source_change_id")
        .withColumn("valid_to", F.coalesce(F.lead("valid_from").over(by_time), far_future))
        .withColumn("version", F.row_number().over(by_time))
        .withColumn("is_current", F.col("valid_to") == far_future)
    )
    return versions.select(*SCD2_COLUMNS)
