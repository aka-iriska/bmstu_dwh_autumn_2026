from __future__ import annotations

import json
from datetime import date, datetime

from pyspark import RDD

from cinema_dwh.transforms.marts import TOP_N
from cinema_dwh.transforms.words import tokenize

# Требование 5: посчитать слова и топ-5 ещё раз, но на RDD - «низкоуровневом» Spark,
# где мы сами работаем со строками и парами (ключ, значение), - и сверить с DataFrame-версией.
# Вход - RDD строк: каждая строка = одно сырое JSON-сообщение из файлов архива raw/events/.

TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"   # 2026-01-01T00:00:00Z


def _event_date(event: dict) -> date:
    """Сутки события по его event_time (UTC), например 2026-01-01."""
    return datetime.strptime(event["event_time"], TIME_FORMAT).date()


def _valid_time(text) -> bool:
    """Строка времени в формате 2026-01-01T00:00:00Z? (None или мусор -> False)."""
    try:
        datetime.strptime(text, TIME_FORMAT)
        return True
    except (TypeError, ValueError):
        return False


def is_valid_event(e: dict) -> bool:
    """Годится ли событие для расчёта - те же правила, что у DataFrame (transforms/events.py).

    Иначе строка, которую DataFrame отбраковал, попала бы в RDD-расчёт и сверка ложно разошлась бы.
    """
    # общие поля у всех событий
    if not (e.get("event_id") or "").strip() or not _valid_time(e.get("event_time")) or e.get("user_id") is None:
        return False
    if e.get("event_type") == "view":   # content_id = 0 валиден: проверяем только на None
        watch = e.get("watch_time_seconds")
        return e.get("content_id") is not None and watch is not None and watch >= 0 and e.get("query") is None
    if e.get("event_type") == "search":
        return bool((e.get("query") or "").strip()) and e.get("content_id") is None \
            and e.get("watch_time_seconds") is None
    return False   # неизвестный event_type


# RDD-версия намеренно независима от DataFrame: сама читает сырой JSON и сама убирает повторы
def unique_events(lines: RDD, dates: set[date]) -> RDD:
    """Из сырых строк JSON - корректные события без повторов, только за сутки dates."""
    events = (lines.filter(str.strip)       # пустые строки файла пропускаем
              .map(json.loads)              # строка -> dict
              .filter(is_valid_event)
              .map(lambda e: (e["event_id"], e)))   # пара (ключ = event_id, значение = событие)
    # reduceByKey собирает все пары с одним event_id и оставляет одну - так убираются повторные доставки
    return events.reduceByKey(lambda first, _repeat: first).values().filter(lambda e: _event_date(e) in dates)


def search_words_rdd(lines: RDD, dates: set[date]) -> list[tuple[date, str, int]]:
    """Частоты поисковых слов: [(сутки, слово, сколько раз)], отсортировано."""
    searches = unique_events(lines, dates).filter(lambda e: e["event_type"] == "search")
    # каждое слово каждого запроса -> пара ((сутки, слово), 1)
    pairs = searches.flatMap(lambda e: [((_event_date(e), word), 1) for word in tokenize(e["query"])])
    # reduceByKey складывает единицы по одинаковым (сутки, слово) -> частота слова
    return sorted((day, word, count) for (day, word), count in pairs.reduceByKey(lambda a, b: a + b).collect())


def _top(day_counts: list[tuple[int, int]]) -> list[tuple[int, int, int]]:
    """Из [(content_id, просмотры)] одних суток -> топ-5 [(место, content_id, просмотры)].

    Порядок: больше просмотров выше; при равенстве - меньший content_id выше.
    """
    ordered = sorted(day_counts, key=lambda item: (-item[1], item[0]))[:TOP_N]
    return [(rank, content_id, views) for rank, (content_id, views) in enumerate(ordered, start=1)]


def top_content_rdd(lines: RDD, dates: set[date]) -> list[tuple[date, int, int, int]]:
    """Топ-5 контента за каждые сутки: [(сутки, место, content_id, просмотры)], отсортировано."""
    views = unique_events(lines, dates).filter(lambda e: e["event_type"] == "view")
    # число просмотров каждого фильма за сутки: ((сутки, content_id), N)
    counts = views.map(lambda e: ((_event_date(e), e["content_id"]), 1)).reduceByKey(lambda a, b: a + b)
    # перегруппировать по суткам: (сутки, [(content_id, N), ...])
    by_day = counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1]))).groupByKey()
    # в каждых сутках оставить топ-5 и развернуть обратно в строки
    ranked = by_day.flatMap(lambda kv: [(kv[0], *row) for row in _top(list(kv[1]))])
    return sorted(ranked.collect())
