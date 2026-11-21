from __future__ import annotations

from datetime import date, datetime, timezone

from cinema_dwh.run_context import META_COLUMNS


def D(day: int) -> date:
    """D(1) == date(2026, 1, 1)."""
    return date(2026, 1, day)


def T(iso: str) -> datetime:
    """T('2026-01-01T12:00:00') - время UTC."""
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def dt(day_iso: str) -> datetime:
    """dt('2026-01-01') - полночь UTC этих суток."""
    return T(f"{day_iso}T00:00:00")


def rows_list(df, drop=()) -> list[tuple]:
    """Строки без метаполей (и без drop), отсортированные."""
    cols = [c for c in df.columns if c not in META_COLUMNS and c not in drop]
    return sorted(tuple(r) for r in df.select(*cols).collect())


def rows(df, drop=()) -> set[tuple]:
    """То же, но множеством."""
    return set(rows_list(df, drop))


def ranks(top_df, day: date) -> list[tuple]:
    """[(rank, content_id, views_count)] топа за сутки."""
    picked = top_df.filter(top_df.event_date == day).orderBy("rank")
    return [(r["rank"], r["content_id"], r["views_count"]) for r in picked.collect()]


def value(views_minute_df, minute: str, content_id: int) -> tuple:
    """(views_count, watch_time_seconds_sum) за минуту и контент."""
    row = (views_minute_df.filter((views_minute_df.minute_start == T(minute.replace(" ", "T")))
                                  & (views_minute_df.content_id == content_id)).collect())
    return (row[0]["views_count"], row[0]["watch_time_seconds_sum"]) if row else (0, 0)


def total(views_minute_df) -> tuple:
    """Сумма просмотров и секунд по всей витрине."""
    agg = views_minute_df.groupBy().sum("views_count", "watch_time_seconds_sum").collect()[0]
    return (agg[0], agg[1])
