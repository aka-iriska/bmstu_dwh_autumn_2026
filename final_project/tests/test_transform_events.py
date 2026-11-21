import pytest

from cinema_dwh.schemas import EVENT_SCHEMA, read_events, read_profile_changes
from cinema_dwh.transforms.events import (DataConflictError, dedup_events, dedup_profile_changes,
                                          validate_events, validate_profile_changes)


def _events(spark, rows):
    """DataFrame событий из кортежей (event_time - строкой, как во входных данных)."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame(rows, EVENT_SCHEMA)
    return df.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ssX"))


VIEW = ("e1", "2026-01-01T00:01:00Z", 1, "view", 0, 60, None, "2026-01-01T00:01:05Z")


def test_fixture_events_dedup(spark, fixture_dir):
    valid, rejected = validate_events(read_events(spark, [str(fixture_dir / "events.jsonl")]))
    clean = dedup_events(valid)
    assert rejected.count() == 0
    assert clean.count() == 26
    assert clean.filter("event_type = 'view'").count() == 20
    assert clean.filter("event_id = '38b65ed6e0b0-e00000001'").count() == 1
    assert "available_at" not in clean.columns and "event_date" in clean.columns


def test_content_id_zero_is_valid(spark):
    valid, rejected = validate_events(_events(spark, [VIEW]))
    assert valid.count() == 1 and rejected.count() == 0


@pytest.mark.parametrize("row,reason", [
    (("e2", "2026-01-01T00:01:00Z", 1, "view", None, 60, None, None), "view without content_id"),
    (("e3", "2026-01-01T00:01:00Z", 1, "click", None, None, None, None), "unknown event_type"),
    (("e4", "bad-time", 1, "view", 1, 60, None, None), "missing or bad event_time"),
    (("e5", "2026-01-01T00:01:00Z", 1, "search", None, None, "  ", None), "search without query"),
    (("e6", "2026-01-01T00:01:00Z", 1, "view", 1, -5, None, None), "view without valid watch_time_seconds"),
])
def test_rejects_with_reason(spark, row, reason):
    valid, rejected = validate_events(_events(spark, [row]))
    assert valid.count() == 0
    assert rejected.collect()[0]["reject_reason"] == reason


def test_conflicting_duplicate_raises(spark):
    other = VIEW[:5] + (99,) + VIEW[6:]
    with pytest.raises(DataConflictError):
        dedup_events(_events(spark, [VIEW, other]))


def test_fixture_profiles_dedup(spark, fixture_dir):
    valid, rejected = validate_profile_changes(read_profile_changes(spark, [str(fixture_dir / "profile_changes.jsonl")]))
    assert rejected.count() == 0
    assert dedup_profile_changes(valid).count() == 8
