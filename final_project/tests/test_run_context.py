from datetime import date, datetime, timezone

import pytest

from cinema_dwh.paths import checks_key, dataset_key, mart_key, s3a, snapshot_key
from cinema_dwh.run_context import interval_days, make_run_id, validate_interval
from cinema_dwh.schemas import read_catalogue, read_events, read_profile_changes
from tests.helpers import dt

AIRFLOW_RUN = "scheduled__2026-01-01T00:00:00+00:00"


def test_make_run_id_is_stable():
    start, end = dt("2026-01-01"), dt("2026-01-02")
    rid = make_run_id(start, end, AIRFLOW_RUN)
    assert rid.startswith("b20260101_") and len(rid) == len("b20260101_") + 8
    assert rid == make_run_id(start, end, AIRFLOW_RUN)                # повтор того же DAG run
    assert rid != make_run_id(start, end, "manual__2026-11-21")       # другой DAG run
    assert rid != make_run_id(start, dt("2026-01-04"), AIRFLOW_RUN)   # тот же DAG run, другой период


@pytest.mark.parametrize("start,end", [
    ("2026-01-01T01:00:00", "2026-01-02T00:00:00"),   # не полночь
    ("2026-01-02T00:00:00", "2026-01-01T00:00:00"),   # начало позже конца
    ("2025-12-31T00:00:00", "2026-01-01T00:00:00"),   # раньше BATCH_START
    ("2026-01-31T00:00:00", "2026-02-02T00:00:00"),   # позже января
])
def test_validate_interval_rejects(start, end):
    with pytest.raises(ValueError):
        validate_interval(datetime.fromisoformat(start + "+00:00"), datetime.fromisoformat(end + "+00:00"))


def test_validate_interval_accepts_whole_days():
    validate_interval(dt("2026-01-01"), dt("2026-01-04"))


def test_interval_days():
    assert interval_days(dt("2026-01-01"), dt("2026-01-03")) == [date(2026, 1, 1), date(2026, 1, 2)]


def test_paths():
    assert snapshot_key("b1") == "batch/b1/snapshot.json"
    assert dataset_key("b1", "clean_events") == "batch/b1/clean_events/"
    assert mart_key("b1", "views_minute") == "batch/b1/marts/views_minute/"
    assert checks_key("b1", "quality") == "batch/b1/checks/quality.json"
    assert s3a("raw/x", "cinema") == "s3a://cinema/raw/x"
    with pytest.raises(ValueError):
        mart_key("b1", "unknown")


def test_read_inputs(spark, fixture_dir):
    events = read_events(spark, [str(fixture_dir / "events.jsonl")])
    assert events.count() == 28
    assert events.filter("content_id = 0").count() == 6   # e00000001 ×3, e8, e15, e20
    assert events.filter("event_time IS NULL").count() == 0
    profiles = read_profile_changes(spark, [str(fixture_dir / "profile_changes.jsonl")])
    assert profiles.count() == 9 and profiles.filter("effective_at IS NULL").count() == 0
    assert read_catalogue(spark, str(fixture_dir / "catalogue.jsonl")).count() == 200
