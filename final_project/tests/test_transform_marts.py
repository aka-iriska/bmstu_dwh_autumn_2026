from cinema_dwh.transforms.words import tokenize
from tests.helpers import D, ranks, rows, total, value


def test_country_tier_point_in_time(marts_full):
    # 1001: RU/basic до 12:00 1 января, KZ/basic до конца дня (позднее p00000007), RU/premium 2-го, RU/basic 3-го.
    assert rows(marts_full["views_country_tier_daily"]) == {
        (D(1), "RU", "basic", 8, 788), (D(1), "KZ", "basic", 4, 597),
        (D(2), "RU", "premium", 6, 1251), (D(3), "RU", "basic", 2, 505)}


def test_country_tier_before_late_profile(marts_stage1):
    assert (D(1), "RU", "basic", 11, 1265) in rows(marts_stage1["views_country_tier_daily"])


def test_boundary_views_go_to_new_version(marts_full):
    # 11:59:59 - старая версия (RU), 12:00:00 и 12:00:01 - новая (KZ).
    enriched = {r.event_id[-3:]: r.country for r in marts_full["views_enriched"].collect()}
    assert (enriched["008"], enriched["009"], enriched["010"]) == ("RU", "KZ", "KZ")


def test_top5_jan1(marts_full):
    assert ranks(marts_full["top_content_daily"], D(1)) == [(1, 0, 3), (2, 1, 2), (3, 2, 2), (4, 3, 2), (5, 4, 1)]


def test_top5_jan2_ties_by_content_id(marts_full):
    assert [c for _, c, _ in ranks(marts_full["top_content_daily"], D(2))] == [0, 1, 2, 4, 5]


def test_views_minute_dedup_and_zero(marts_full):
    assert value(marts_full["views_minute"], "2026-01-01 00:01:00", 0) == (1, 60)
    assert total(marts_full["views_minute"]) == (20, 3141)


def test_views_minute_stage1(marts_stage1):
    assert total(marts_stage1["views_minute"]) == (19, 3021)


def test_search_words_jan1(marts_full):
    words = rows(marts_full["search_words_daily"])
    assert len(words) == 11
    assert (D(1), "кино", 1) not in words and (D(1), "документальное", 1) in words


def test_tokenize_unicode_spaces():
    assert tokenize("Семейная КОМЕДИЯ  кино") == ["семейная", "комедия"]
    assert tokenize("  драма драма драма ") == ["драма", "драма", "драма"]
