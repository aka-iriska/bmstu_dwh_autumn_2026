import pytest
from pyspark.sql import functions as F

from cinema_dwh.checks import QualityCheckError, assert_all_ok, run_quality_checks

MART_NAMES = ("views_minute", "views_country_tier_daily", "top_content_daily", "search_words_daily")


def _run(data, **override):
    """Запустить все проверки на данных fixture, подменив отдельные наборы."""
    parts = {**data, **override}
    marts = {name: parts[name] for name in MART_NAMES}
    return run_quality_checks(parts["clean_events"], parts["catalogue"], parts["scd2"], parts["views_enriched"], marts)


def _failed(report):
    return {c["name"] for c in report["checks"] if not c["ok"]}


def test_fixture_passes_all(marts_full):
    report = _run(marts_full)
    assert report["ok"], report
    assert_all_ok(report)


def test_duplicate_mart_key_detected(marts_full):
    views_minute = marts_full["views_minute"]
    report = _run(marts_full, views_minute=views_minute.unionByName(views_minute.limit(1)))
    assert "unique_keys_views_minute" in _failed(report)
    with pytest.raises(QualityCheckError):
        assert_all_ok(report)


def test_overlapping_scd2_detected(marts_full):
    scd2 = marts_full["scd2"]
    broken = scd2.withColumn("valid_to", F.when(F.col("version") == 1, F.col("valid_to") + F.expr("INTERVAL 1 HOUR"))
                             .otherwise(F.col("valid_to")))
    # Пересечение версий: часть просмотров попадает сразу в две версии.
    assert {"scd2_no_overlap", "one_profile_version"} <= _failed(_run(marts_full, scd2=broken))


def test_view_without_catalogue_item_detected(marts_full):
    catalogue = marts_full["catalogue"].filter("content_id != 1")
    assert "one_catalogue_item" in _failed(_run(marts_full, catalogue=catalogue))


def test_lost_view_detected(marts_full):
    enriched = marts_full["views_enriched"].filter("event_id != '38b65ed6e0b0-e00000002'")
    assert "views_reconcile" in _failed(_run(marts_full, views_enriched=enriched))
