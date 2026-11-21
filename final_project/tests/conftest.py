from __future__ import annotations

from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parent.parent


# тесты запускаются в контейнере: docker compose run --rm tools pytest -q
def pytest_configure(config):
    config.addinivalue_line("markers", "integration: нужен поднятый стенд (ClickHouse, S3)")


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    """Готовый контрольный набор из задания."""
    return PROJECT / "data" / "fixture"


CUTOFF = "2026-01-04T00:00:00Z"   # граница двух этапов отправки готовых наборов


def build_fixture_marts(spark, fixture_dir: Path, until: str | None = None) -> dict:
    """Весь пакетный расчёт на файлах fixture за 1–3 января (until - только сообщения раньше него)."""
    from pyspark.sql import functions as F

    from cinema_dwh.schemas import read_catalogue, read_events, read_profile_changes
    from cinema_dwh.transforms.events import (dedup_events, dedup_profile_changes, validate_events,
                                              validate_profile_changes)
    from cinema_dwh.transforms.marts import build_all_marts
    from cinema_dwh.transforms.scd2 import build_scd2

    events = read_events(spark, [str(fixture_dir / "events.jsonl")])
    profiles = read_profile_changes(spark, [str(fixture_dir / "profile_changes.jsonl")])
    if until:
        events, profiles = events.filter(F.col("available_at") < until), profiles.filter(F.col("available_at") < until)
    clean = dedup_events(validate_events(events)[0]).cache()
    scd2 = build_scd2(dedup_profile_changes(validate_profile_changes(profiles)[0])).cache()
    catalogue = read_catalogue(spark, str(fixture_dir / "catalogue.jsonl")).cache()
    enriched, marts = build_all_marts(clean, catalogue, scd2)
    return {"clean_events": clean, "catalogue": catalogue, "scd2": scd2, "views_enriched": enriched.cache(),
            **{name: df.cache() for name, df in marts.items()}}


@pytest.fixture(scope="session")
def marts_full(spark, fixture_dir) -> dict:
    """Расчёт по всем сообщениям fixture (после второй отправки)."""
    return build_fixture_marts(spark, fixture_dir)


@pytest.fixture(scope="session")
def marts_stage1(spark, fixture_dir) -> dict:
    """Расчёт по первой части fixture: без позднего e00000020 и p00000007."""
    return build_fixture_marts(spark, fixture_dir, until=CUTOFF)


@pytest.fixture(scope="session")
def spark():
    """Одна локальная Spark-сессия на все тесты."""
    from cinema_dwh.spark import create_spark

    session = create_spark("tests", master="local[1]", with_s3=False)
    yield session
    session.stop()
