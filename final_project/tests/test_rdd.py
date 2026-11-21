from cinema_dwh.checks import compare_rows
from cinema_dwh.rdd.marts_rdd import search_words_rdd, top_content_rdd
from tests.helpers import D, rows_list

DATES = {D(1), D(2), D(3)}


def test_rdd_equals_dataframe_on_fixture(spark, fixture_dir, marts_full):
    lines = spark.sparkContext.textFile(str(fixture_dir / "events.jsonl"))
    words = compare_rows("search_words_daily", search_words_rdd(lines, DATES),
                         rows_list(marts_full["search_words_daily"]))
    top = compare_rows("top_content_daily", top_content_rdd(lines, DATES),
                       rows_list(marts_full["top_content_daily"], drop=["category"]))
    assert words["ok"] and words["rdd_rows"] == 11, words
    assert top["ok"] and top["rdd_rows"] == 5 + 5 + 2, top


def test_rdd_validation_matches_dataframe_rules():
    from cinema_dwh.rdd.marts_rdd import is_valid_event

    base = {"event_id": "e", "event_time": "2026-01-01T00:00:00Z", "user_id": 1}
    assert is_valid_event({**base, "event_type": "view", "content_id": 0, "watch_time_seconds": 5, "query": None})
    assert not is_valid_event({**base, "event_type": "view", "content_id": None, "watch_time_seconds": 5})
    assert not is_valid_event({**base, "event_type": "search", "query": "  "})
    assert not is_valid_event({**base, "event_time": "bad", "event_type": "search", "query": "кино"})


def test_compare_reports_mismatch():
    rep = compare_rows("x", [(1, "a", 1)], [(1, "a", 2)])
    assert not rep["ok"] and len(rep["mismatches"]) == 2
