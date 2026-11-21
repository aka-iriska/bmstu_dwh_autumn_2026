import pytest

from cinema_dwh.clickhouse import StaleSnapshotError, assert_snapshot_not_stale, next_boundary, published_days
from cinema_dwh.config import BATCH_START
from tests.helpers import D, T, dt


def test_first_publish_jan1_moves_t_to_jan2():
    assert next_boundary(None, {D(1)}, BATCH_START) == T("2026-01-02T00:00:00")


def test_jan2_without_jan1_does_not_move():
    assert next_boundary(None, {D(2)}, BATCH_START) is None


def test_chain_moves_over_contiguous_days_only():
    assert next_boundary(T("2026-01-02T00:00:00"), {D(1), D(2), D(3), D(5)}, BATCH_START) == T("2026-01-04T00:00:00")


def test_t_never_moves_back():
    assert next_boundary(T("2026-01-04T00:00:00"), {D(1)}, BATCH_START) == T("2026-01-04T00:00:00")


def test_published_days_include_affected():
    runs = [{"interval_start": dt("2026-01-04"), "interval_end": dt("2026-01-05"), "affected_dates": [D(1), D(4)]}]
    assert published_days(runs) == {D(1), D(4)}


def test_stale_snapshot_rejected():
    runs = [{"snapshot_created_at": T("2026-11-21T12:10:00")}]
    with pytest.raises(StaleSnapshotError):
        assert_snapshot_not_stale(T("2026-11-21T12:00:00"), runs)
    assert_snapshot_not_stale(T("2026-11-21T12:10:00"), runs)   # повтор того же среза - можно
    assert_snapshot_not_stale(T("2026-11-21T12:00:00"), [])     # публикаций ещё нет
