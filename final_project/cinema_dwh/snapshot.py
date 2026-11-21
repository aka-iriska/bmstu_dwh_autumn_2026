from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta, timezone

from cinema_dwh import s3io
from cinema_dwh.paths import CATALOGUE_KEY, RAW_EVENTS_PREFIX, RAW_PROFILES_PREFIX, snapshot_key
from cinema_dwh.run_context import RunMeta, to_iso

# Даты можно передать просто множеством {date, ...} или словарём {date: [ID, ...]} - тогда ID попадут в причины
DatesWithIds = Mapping[date, Iterable[str]] | Iterable[date]


def new_files(current: list[str], published: list[list[str]]) -> list[str]:
    """Какие файлы архива пришли после прошлых публикаций.

    :param current: Файлы текущего среза.
    :param published: Списки файлов срезов всех опубликованных запусков.
    :returns: Файлы current, которых нет ни в одном из них (по ним видно, что пришло нового).
    """
    seen = {key for files in published for key in files}
    return [key for key in current if key not in seen]


def _as_map(dates: DatesWithIds) -> dict[date, list[str]]:
    """Множество дат или словарь «дата -> ID» -> всегда словарь (у множества списки ID пустые)."""
    if isinstance(dates, Mapping):
        return {d: sorted(ids) for d, ids in dates.items()}
    return {d: [] for d in dates}


def _days(first: date, last: date) -> list[date]:
    """Все сутки от first до last включительно."""
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def affected_dates(interval_start: date, interval_end: date, event_dates: DatesWithIds,
                   profile_effective_dates: DatesWithIds,
                   published_dates: set[date]) -> tuple[list[date], list[dict]]:
    """Какие сутки пересчитать этому запуску и почему.

    :param event_dates: Сутки событий из новых файлов.
    :param profile_effective_dates: С каких суток действуют новые изменения профилей.
    :param published_dates: Уже опубликованные сутки.
    :returns: (список суток, список причин вида {"date", "why", "ids"}).
    """
    last_interval_day = interval_end - timedelta(days=1)   # интервал [начало, конец) - конец не входит
    # Изменение профиля действует на все дни после effective_at, включая опубликованные позже интервала.
    horizon = max([last_interval_day, *published_dates])

    # причина 1: сутки самого интервала запуска - всегда
    reasons = [{"date": d.isoformat(), "why": "interval"} for d in _days(interval_start, last_interval_day)]
    # причина 2: позднее/новое событие задевает свои сутки
    for day, ids in sorted(_as_map(event_dates).items()):
        reasons.append({"date": day.isoformat(), "why": "new_event", "ids": ids})
    # причина 3: новое изменение профиля задевает все сутки от effective_at до horizon
    for effective, ids in sorted(_as_map(profile_effective_dates).items()):
        reasons.extend({"date": d.isoformat(), "why": "new_profile_change", "ids": ids}
                       for d in _days(effective, horizon))
    # Будущие сутки, которые ещё ни разу не публиковались, целиком посчитает их собственный запуск.
    kept = [r for r in reasons if date.fromisoformat(r["date"]) < interval_end
            or date.fromisoformat(r["date"]) in published_dates]
    return sorted({date.fromisoformat(r["date"]) for r in kept}), kept


def build_snapshot(meta: RunMeta, event_files: list[str], profile_files: list[str],
                   published_file_lists: list[list[str]], created_at: datetime) -> dict:
    """Содержимое snapshot.json: интервал запуска, все файлы архива и какие из них новые."""
    return {
        "run_id": meta.run_id,
        "interval_start": to_iso(meta.interval_start),
        "interval_end": to_iso(meta.interval_end),
        "created_at": to_iso(created_at),   # по нему защита от отката сравнивает «свежесть» срезов
        "files": {"events": event_files, "profile_changes": profile_files, "catalogue": CATALOGUE_KEY},
        "new_files": new_files(event_files + profile_files, published_file_lists),
    }


def freeze(meta: RunMeta, published_file_lists: list[list[str]]) -> dict:
    """Создать batch/<run_id>/snapshot.json.

    Если он уже есть (повтор задачи) - вернуть сохранённый без изменений."""
    key = snapshot_key(meta.run_id)
    if s3io.exists(key):   # повтор того же запуска - читаем тот же срез, иначе цифры могли бы поменяться
        stored = s3io.read_json(key)
        # Срез принадлежит именно этому интервалу (run_id зависит от интервала, это страховка).
        if (stored["interval_start"], stored["interval_end"]) != (to_iso(meta.interval_start), to_iso(meta.interval_end)):
            raise ValueError(f"срез {key} сделан для другого интервала: {stored['interval_start']} - {stored['interval_end']}")
        return stored

    # срез - только имена файлов raw/ на этот момент: повтор задачи читает те же файлы, результат воспроизводим
    snapshot = build_snapshot(meta = meta,
                              event_files=s3io.list_keys(RAW_EVENTS_PREFIX),
                              profile_files = s3io.list_keys(RAW_PROFILES_PREFIX),
                              published_file_lists = published_file_lists,
                              created_at = datetime.now(timezone.utc))
    s3io.write_json(key, snapshot)
    return snapshot
