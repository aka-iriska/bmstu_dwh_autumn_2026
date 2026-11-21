from cinema_dwh.config import FAR_FUTURE
from cinema_dwh.schemas import read_profile_changes
from cinema_dwh.transforms.events import dedup_profile_changes, validate_profile_changes
from cinema_dwh.transforms.scd2 import build_scd2
from tests.helpers import T

CUTOFF = "2026-01-04T00:00:00Z"


def scd2_rows(spark, fixture_dir, until=None):
    """Отсортированные кортежи (user, country, tier, valid_from, valid_to, version)."""
    df = read_profile_changes(spark, [str(fixture_dir / "profile_changes.jsonl")])
    if until:
        df = df.filter(df.available_at < until)
    valid, _ = validate_profile_changes(df)
    scd = build_scd2(dedup_profile_changes(valid))
    rows = [(r.user_id, r.country, r.subscription_tier, r.valid_from.replace(tzinfo=FAR_FUTURE.tzinfo),
             r.valid_to.replace(tzinfo=FAR_FUTURE.tzinfo), r.version) for r in scd.collect()]
    return sorted(rows, key=lambda r: (r[0], r[3])), scd   # по пользователю и началу версии


def test_fixture_full_history(spark, fixture_dir):
    rows, _ = scd2_rows(spark, fixture_dir)
    assert [r for r in rows if r[0] == 1001] == [
        (1001, "RU", "basic", T("2026-01-01T00:00:00"), T("2026-01-01T12:00:00"), 1),
        (1001, "KZ", "basic", T("2026-01-01T12:00:00"), T("2026-01-02T00:00:00"), 2),
        (1001, "RU", "premium", T("2026-01-02T00:00:00"), T("2026-01-03T00:00:00"), 3),
        (1001, "RU", "basic", T("2026-01-03T00:00:00"), FAR_FUTURE, 4),
    ]
    assert len(rows) == 7     # 1002: BY -> AM, 1003: одна версия


def test_without_late_change(spark, fixture_dir):
    rows, _ = scd2_rows(spark, fixture_dir, until=CUTOFF)
    assert [(r[1], r[2], r[3]) for r in rows if r[0] == 1001] == [
        ("RU", "basic", T("2026-01-01T00:00:00")),
        ("RU", "premium", T("2026-01-02T00:00:00")),   # p00000005 (то же состояние) версию не создал
        ("RU", "basic", T("2026-01-03T00:00:00")),
    ]


def test_exactly_one_current_per_user(spark, fixture_dir):
    _, scd = scd2_rows(spark, fixture_dir)
    current = scd.filter("is_current").groupBy("user_id").count().collect()
    assert sorted((r.user_id, r["count"]) for r in current) == [(1001, 1), (1002, 1), (1003, 1)]
