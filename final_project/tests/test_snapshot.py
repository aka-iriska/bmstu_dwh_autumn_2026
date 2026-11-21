from datetime import datetime, timezone

import pytest

from cinema_dwh import s3io
from cinema_dwh.run_context import RunMeta
from cinema_dwh.snapshot import affected_dates, freeze, new_files
from tests.helpers import D, dt


def test_new_files_ignores_any_published():
    assert new_files(["a", "b", "c"], [["a"], ["b"]]) == ["c"]
    assert new_files(["a"], []) == ["a"]


def test_first_run_only_its_interval():
    # Архив уже содержит события 1–3 января, но опубликованных дней нет: считаем только 1 января.
    dates, _ = affected_dates(D(1), D(2), {D(1), D(2), D(3)}, {D(1)}, set())
    assert dates == [D(1)]


def test_late_data_run_jan4_recomputes_history():
    # fixture после второй отправки: e00000020 (1 января) и p00000007 (1001 -> KZ с 1 января 12:00).
    dates, reasons = affected_dates(D(4), D(5), {D(1): ["38b65ed6e0b0-e00000020"]},
                                    {D(1): ["38b65ed6e0b0-p00000007"]}, {D(1), D(2), D(3)})
    assert dates == [D(1), D(2), D(3), D(4)]
    assert {"date": "2026-01-01", "why": "new_event", "ids": ["38b65ed6e0b0-e00000020"]} in reasons
    assert {"date": "2026-01-04", "why": "interval"} in reasons


def test_late_event_for_published_future_day_kept():
    # Ручной пересчёт 1 января после публикации 2 января: позднее событие за 2 января не теряется.
    dates, _ = affected_dates(D(1), D(2), {D(2)}, set(), {D(1), D(2)})
    assert dates == [D(1), D(2)]


def test_late_profile_change_reaches_published_future_days():
    dates, _ = affected_dates(D(1), D(2), set(), {D(1)}, {D(1), D(2), D(3)})
    assert dates == [D(1), D(2), D(3)]


def test_unpublished_future_days_dropped():
    dates, _ = affected_dates(D(2), D(3), {D(5)}, set(), {D(1), D(2)})
    assert dates == [D(2)]


@pytest.fixture
def fake_s3(monkeypatch):
    """S3 в памяти: словарь ключ -> объект."""
    store = {"raw/events/a.jsonl": None, "raw/events/b.jsonl": None, "raw/profile_changes/p.jsonl": None}
    monkeypatch.setattr(s3io, "list_keys", lambda prefix: sorted(k for k in store if k.startswith(prefix)))
    monkeypatch.setattr(s3io, "exists", lambda key: key in store)
    monkeypatch.setattr(s3io, "read_json", lambda key: store[key])
    monkeypatch.setattr(s3io, "write_json", lambda key, obj: store.__setitem__(key, obj))
    return store


def test_freeze_is_idempotent(fake_s3):
    meta = RunMeta("b20260101_x", dt("2026-01-01"), dt("2026-01-02"), datetime.now(timezone.utc))
    first = freeze(meta, [["raw/events/a.jsonl"]])
    assert first["files"]["events"] == ["raw/events/a.jsonl", "raw/events/b.jsonl"]
    assert first["new_files"] == ["raw/events/b.jsonl", "raw/profile_changes/p.jsonl"]
    fake_s3["raw/events/c.jsonl"] = None          # пришёл новый файл после среза
    assert freeze(meta, []) == first              # повтор задачи - тот же срез, тот же created_at


def test_freeze_rejects_snapshot_of_other_interval(fake_s3):
    meta = RunMeta("b20260101_x", dt("2026-01-01"), dt("2026-01-02"), datetime.now(timezone.utc))
    freeze(meta, [])
    other = RunMeta("b20260101_x", dt("2026-01-01"), dt("2026-01-04"), datetime.now(timezone.utc))
    with pytest.raises(ValueError):
        freeze(other, [])
