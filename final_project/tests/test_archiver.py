from datetime import date, datetime, timezone

import pytest

from archiver.archiver import batch_key, entity_for_topic, envelope
from archiver.batch_buffer import BatchBuffer


def test_batch_key():
    assert batch_key("events", date(2026, 11, 21), 2, 10, 14) == \
        "raw/events/ingest_date=2026-11-21/part-p2-o10-14.jsonl"


def test_entity_for_topic():
    assert entity_for_topic("cinema.profile_changes") == "profile_changes"
    with pytest.raises(ValueError):
        entity_for_topic("other")


def test_envelope_keeps_payload_and_adds_kafka_fields():
    rec = envelope(b'{"event_id":"x","content_id":0}', "cinema.events", 1, 7,
                   datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc))
    assert rec["content_id"] == 0 and rec["event_id"] == "x"
    assert rec["kafka_topic"] == "cinema.events" and rec["kafka_partition"] == 1 and rec["kafka_offset"] == 7
    assert rec["archived_at"] == "2026-11-21T12:00:00Z"


def test_buffer_flushes_by_size_and_age():
    buf = BatchBuffer(max_messages=2, max_age_seconds=10)
    buf.add("cinema.events", 0, {"a": 1}, now=0)
    assert buf.ready(now=1) == []
    buf.add("cinema.events", 0, {"a": 2}, now=1)
    assert buf.ready(now=1) == [("cinema.events", 0)]
    buf.add("cinema.events", 1, {"a": 3}, now=0)
    assert ("cinema.events", 1) in buf.ready(now=11)
    assert buf.drain("cinema.events", 0) == [{"a": 1}, {"a": 2}]
    assert buf.drain("cinema.events", 0) == []


def test_buffer_drops_revoked_partitions():
    buf = BatchBuffer(max_messages=10, max_age_seconds=10)
    buf.add("cinema.events", 0, {"a": 1}, now=0)
    buf.add("cinema.events", 1, {"a": 2}, now=0)
    buf.drop([("cinema.events", 0)])
    assert buf.ready(now=100) == [("cinema.events", 1)]


def test_non_json_message_is_skipped():
    from archiver.archiver import accept

    class Message:   # минимальная подмена сообщения Kafka
        def value(self): return b"not json"
        def topic(self): return "cinema.events"
        def partition(self): return 0
        def offset(self): return 5

    buf = BatchBuffer()
    accept(Message(), buf, now=0)
    assert buf.ready(now=100) == []
