from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from cinema_dwh.transforms.words import MIN_WORD_LEN, TOKEN_SPLIT_REGEX

# Шаг 4 DAG в виде чистых функций: таблица Spark на входе -> таблица Spark на выходе (без чтения/записи файлов).
# Витрина - готовая таблица для отчёта; у нас их 4, каждая потом публикуется в ClickHouse как batch_<имя>.

# Колонки views_enriched: поля просмотра + из каталога (category, duration_seconds)
# + из версии профиля на момент просмотра (country, subscription_tier, profile_version)
ENRICHED_COLUMNS = ["event_id", "event_time", "event_date", "user_id", "content_id", "watch_time_seconds",
                    "category", "duration_seconds", "country", "subscription_tier", "profile_version"]
TOP_N = 5   # размер дневного топа


def enrich_views(events: DataFrame, catalogue: DataFrame, scd2: DataFrame) -> DataFrame:
    """Каждый просмотр + данные фильма из каталога + страна и тариф пользователя на момент просмотра."""
    views = events.filter(F.col("event_type") == "view")   # поиски сюда не идут
    # JOIN с каталогом по content_id. Каталог маленький (200 строк) - broadcast рассылает его целиком
    # на каждый поток Spark, поэтому популярный фильм не создаёт перекоса.
    with_catalogue = views.join(F.broadcast(catalogue), "content_id")
    profiles = scd2.select("user_id", "country", "subscription_tier", "valid_from", "valid_to",
                           F.col("version").alias("profile_version")).alias("p")
    # Исторический JOIN: та же персона И время просмотра внутри интервала версии [valid_from, valid_to).
    # Просмотр ровно на границе (valid_from) относится к новой версии.
    on_time = ((F.col("v.user_id") == F.col("p.user_id"))
               & (F.col("p.valid_from") <= F.col("v.event_time"))
               & (F.col("v.event_time") < F.col("p.valid_to")))
    joined = with_catalogue.alias("v").join(profiles, on_time)
    # первые 8 колонок берём от просмотра (v), остальные 3 - от версии профиля (p)
    return joined.select(*[F.col(f"v.{c}") for c in ENRICHED_COLUMNS[:8]],
                         "p.country", "p.subscription_tier", "p.profile_version")


def _views_and_seconds():
    """Две метрики витрин просмотров: views_count (сколько просмотров) и watch_time_seconds_sum (сколько секунд)."""
    return [F.count(F.lit(1)).alias("views_count"), F.sum("watch_time_seconds").alias("watch_time_seconds_sum")]


def views_minute(enriched: DataFrame) -> DataFrame:
    """Витрина «минута × фильм»: сколько просмотров и секунд (те же метрики, что у speed-слоя)."""
    # date_trunc обрезает время до минуты: 12:00:59 -> 12:00:00. Всё время просмотра - в минуту event_time.
    minute = F.date_trunc("minute", "event_time").alias("minute_start")
    return enriched.groupBy(minute, "content_id").agg(*_views_and_seconds())


def views_country_tier_daily(enriched: DataFrame) -> DataFrame:
    """Витрина «сутки × страна × тариф»: страна и тариф - какими были в момент просмотра."""
    return enriched.groupBy("event_date", "country", "subscription_tier").agg(*_views_and_seconds())


def top_content_daily(enriched: DataFrame) -> DataFrame:
    """Витрина «топ-5 фильмов за сутки»: больше просмотров - выше; при равенстве - меньший content_id выше."""
    counts = enriched.groupBy("event_date", "content_id", "category").agg(F.count(F.lit(1)).alias("views_count"))
    # оконная функция: внутри каждых суток пронумеровать фильмы 1, 2, 3... в нужном порядке
    order = Window.partitionBy("event_date").orderBy(F.desc("views_count"), F.asc("content_id"))
    ranked = counts.withColumn("rank", F.row_number().over(order)).filter(F.col("rank") <= TOP_N)
    return ranked.select("event_date", "rank", "content_id", "category", "views_count")


def search_words_daily(events: DataFrame) -> DataFrame:
    """Витрина «сутки × слово»: сколько раз слово встретилось в поисковых запросах (правила - words.py)."""
    searches = events.filter(F.col("event_type") == "search")   # события уже без повторов
    # lower -> нижний регистр; split -> массив слов по пробелам; explode -> по строке на каждое слово
    words = searches.select("event_date", F.explode(F.split(F.lower("query"), TOKEN_SPLIT_REGEX)).alias("word"))
    long_words = words.filter(F.length("word") >= MIN_WORD_LEN)   # «длиннее 4 символов»; пустые токены отсекаются тут же
    return long_words.groupBy("event_date", "word").agg(F.count(F.lit(1)).alias("word_count"))


def build_all_marts(clean_events: DataFrame, catalogue: DataFrame, scd2: DataFrame) -> tuple[DataFrame, dict]:
    """Посчитать views_enriched и все 4 витрины; витрины - словарь {"views_minute": таблица, ...}."""
    enriched = enrich_views(clean_events, catalogue, scd2)
    marts = {
        "views_minute": views_minute(enriched),
        "views_country_tier_daily": views_country_tier_daily(enriched),
        "top_content_daily": top_content_daily(enriched),
        # поиски идут напрямую из clean_events: в views_enriched только просмотры
        "search_words_daily": search_words_daily(clean_events),
    }
    return enriched, marts   # метаполя (run_id и т.д.) добавляет задача build_marts перед записью
