from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Проверки перед публикацией (шаг 5 DAG). Каждая проверка возвращает словарь
# {"name": имя, "ok": прошла ли, "details": числа для разбора}; весь отчёт пишется в checks/quality.json.

# Ключ витрины - набор колонок, который должен быть уникален (одна строка на «минуту × фильм» и т.д.)
MART_KEYS = {
    "views_minute": ["minute_start", "content_id"],
    "views_country_tier_daily": ["event_date", "country", "subscription_tier"],
    "top_content_daily": ["event_date", "rank"],
    "search_words_daily": ["event_date", "word"],
}


class QualityCheckError(Exception):
    """Хотя бы одна проверка не прошла - публиковать нельзя."""


def compare_rows(name: str, rdd_rows: list[tuple], df_rows: list[tuple]) -> dict:
    """Сверка RDD и DataFrame: совпадают ли строки результата двух реализаций (порядок не важен).

    В mismatches - до 10 строк, которые есть только с одной стороны.
    """
    rdd_set, df_set = set(rdd_rows), set(df_rows)
    mismatches = sorted(rdd_set ^ df_set, key=str)[:10]   # ^ - строки, которые есть только в одном из множеств
    return {"name": name, "ok": rdd_set == df_set and len(rdd_rows) == len(df_rows),
            "rdd_rows": len(rdd_rows), "df_rows": len(df_rows), "mismatches": [list(map(str, m)) for m in mismatches]}


def _check(name: str, ok: bool, details) -> dict:
    """Упаковать результат одной проверки в общий формат {"name", "ok", "details"}."""
    return {"name": name, "ok": bool(ok), "details": details}


def _duplicates(df: DataFrame, keys: list[str]) -> int:
    """Сколько значений ключа keys встречается в таблице больше одного раза (0 - ключ уникален)."""
    return df.groupBy(*keys).count().filter("count > 1").count()


def unique_keys(name: str, df: DataFrame, keys: list[str]) -> dict:
    """Проверка: в таблице name нет двух строк с одинаковым ключом keys."""
    dups = _duplicates(df, keys)
    return _check(f"unique_keys_{name}", dups == 0, {"duplicate_keys": dups})


def scd2_no_overlap(scd2: DataFrame) -> dict:
    """Проверка SCD2: версии каждого пользователя идут встык без пересечений, текущая версия ровно одна,
    соседние версии отличаются страной или тарифом."""
    by_time = Window.partitionBy("user_id").orderBy("valid_from")   # версии пользователя по порядку
    # lead - значение из следующей строки: начало следующей версии и её состояние
    nxt = (scd2.withColumn("next_from", F.lead("valid_from").over(by_time))
               .withColumn("next_state", F.lead(F.struct("country", "subscription_tier")).over(by_time)))
    bad_order = scd2.filter(F.col("valid_from") >= F.col("valid_to")).count()          # версия «нулевой длины»
    # следующая версия должна начинаться ровно там, где кончилась текущая (иначе дыра или наложение)
    gaps = nxt.filter(F.col("next_from").isNotNull() & (F.col("next_from") != F.col("valid_to"))).count()
    same_state = nxt.filter(F.col("next_state") == F.struct("country", "subscription_tier")).count()  # лишняя версия
    currents = scd2.groupBy("user_id").agg(F.sum(F.col("is_current").cast("int")).alias("n")).filter("n != 1").count()
    dups = _duplicates(scd2, ["user_id", "valid_from"])
    details = {"valid_from>=valid_to": bad_order, "gaps_or_overlaps": gaps, "repeated_states": same_state,
               "users_without_single_current": currents, "duplicate_user_valid_from": dups}
    return _check("scd2_no_overlap", not any(details.values()), details)   # все счётчики должны быть 0


def _matches_per_view(views: DataFrame, candidates: DataFrame, condition) -> dict:
    """Для каждого просмотра посчитать, сколько строк candidates ему подходит по condition.

    Возвращает, сколько просмотров не нашли пары (0) и сколько нашли больше одной.
    """
    marked = candidates.withColumn("_hit", F.lit(1))   # метка «нашлась пара»
    # left join: просмотр без пары остаётся со _hit = null -> сумма 0
    per_view = (views.alias("v").join(marked.alias("c"), condition, "left")
                .groupBy("v.event_id").agg(F.coalesce(F.sum("c._hit"), F.lit(0)).alias("n")))
    return {"views_without_match": per_view.filter("n = 0").count(),
            "views_with_several_matches": per_view.filter("n > 1").count()}


def one_catalogue_item(clean_views: DataFrame, catalogue: DataFrame) -> dict:
    """Проверка: content_id каждого просмотра есть в каталоге ровно один раз."""
    counts = _matches_per_view(clean_views, catalogue.select("content_id"),
                               F.col("v.content_id") == F.col("c.content_id"))
    return _check("one_catalogue_item", not any(counts.values()), counts)


def one_profile_version(clean_views: DataFrame, scd2: DataFrame) -> dict:
    """Проверка: у каждого просмотра ровно одна версия профиля, действовавшая в момент просмотра."""
    on_time = ((F.col("v.user_id") == F.col("c.user_id")) & (F.col("c.valid_from") <= F.col("v.event_time"))
               & (F.col("v.event_time") < F.col("c.valid_to")))
    counts = _matches_per_view(clean_views, scd2.select("user_id", "valid_from", "valid_to"), on_time)
    return _check("one_profile_version", not any(counts.values()), counts)


def views_reconcile(clean_views: DataFrame, enriched: DataFrame, marts: dict[str, DataFrame]) -> dict:
    """Проверка: число просмотров одинаково везде - в clean_events, views_enriched и двух витринах.

    Если JOIN где-то потерял просмотр, числа разойдутся.
    """
    def views_sum(df):
        """Сумма колонки views_count витрины (0, если витрина пустая)."""
        return df.agg(F.coalesce(F.sum("views_count"), F.lit(0))).collect()[0][0]

    sums = {"views_minute": views_sum(marts["views_minute"]),
            "views_country_tier_daily": views_sum(marts["views_country_tier_daily"]),
            "views_enriched": enriched.count(), "clean_events_views": clean_views.count()}
    return _check("views_reconcile", len(set(sums.values())) == 1, sums)   # все четыре числа равны


def top_ranks_valid(top: DataFrame) -> dict:
    """Проверка топа: места 1, 2, ..., N без пропусков, N не больше 5, фильм в сутках не повторяется."""
    per_day = top.groupBy("event_date").agg(F.count(F.lit(1)).alias("n"), F.max("rank").alias("max_rank"))
    # если мест n, то последнее место должно быть n (иначе пропуск в нумерации)
    bad_days = per_day.filter((F.col("n") != F.col("max_rank")) | (F.col("n") > 5)).count()
    dup_content = _duplicates(top, ["event_date", "content_id"])
    return _check("top_ranks_valid", bad_days == 0 and dup_content == 0,
                  {"days_with_bad_ranks": bad_days, "repeated_content": dup_content})


def run_quality_checks(clean_events: DataFrame, catalogue: DataFrame, scd2: DataFrame, enriched: DataFrame,
                       marts: dict[str, DataFrame]) -> dict:
    """Прогнать все проверки и собрать отчёт {"checks": [...], "ok": все ли прошли} для checks/quality.json."""
    clean_views = clean_events.filter(F.col("event_type") == "view")
    checks = [unique_keys(name, marts[name], keys) for name, keys in MART_KEYS.items()]
    checks += [unique_keys("clean_events", clean_events, ["event_id"]),
               unique_keys("views_enriched", enriched, ["event_id"]), scd2_no_overlap(scd2),
               one_catalogue_item(clean_views, catalogue), one_profile_version(clean_views, scd2),
               views_reconcile(clean_views, enriched, marts), top_ranks_valid(marts["top_content_daily"])]
    return {"checks": checks, "ok": all(c["ok"] for c in checks)}


def assert_all_ok(report: dict) -> None:
    """Если в отчёте есть непрошедшая проверка - бросить QualityCheckError (задача падает)."""
    failed = [c["name"] for c in report.get("checks", []) if not c["ok"]]
    if failed or not report.get("ok", False):
        # любой ok=False останавливает DAG до публикации
        raise QualityCheckError(f"проверки не прошли: {failed}")
