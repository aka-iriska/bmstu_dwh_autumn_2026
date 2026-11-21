from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition

from archiver.batch_buffer import BatchBuffer
from cinema_dwh import s3io
from cinema_dwh.config import load_settings

TOPICS = {"cinema.events": "events", "cinema.profile_changes": "profile_changes"}
GROUP_ID = "cinema-s3-archiver"
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

log = logging.getLogger("archiver")


def entity_for_topic(topic: str) -> str:
    """Имя папки в raw/ для топика."""
    if topic not in TOPICS:
        raise ValueError(f"неизвестный топик: {topic}")
    return TOPICS[topic]


def envelope(value: bytes, topic: str, partition: int, offset: int, archived_at: datetime) -> dict:
    """Исходное сообщение без изменений + откуда оно пришло и когда заархивировано."""
    record = json.loads(value)
    record.update(kafka_topic=topic, kafka_partition=partition, kafka_offset=offset,
                  archived_at=archived_at.astimezone(timezone.utc).strftime(TIME_FORMAT))
    return record


def batch_key(entity: str, ingest_date: date, partition: int, first_offset: int, last_offset: int) -> str:
    """Имя файла пачки: по нему видно топик, партицию и диапазон offset."""
    return (f"raw/{entity}/ingest_date={ingest_date.isoformat()}/"
            f"part-p{partition}-o{first_offset}-{last_offset}.jsonl")


def write_batch(records: list[dict]) -> str:
    """Записать пачку одной партиции в S3 файлом JSONL; вернуть ключ."""
    first, last = records[0], records[-1]
    # Пишет каждую пачку файлом JSONL в raw/<entity>/: events или profile_changes по топику.
    key = batch_key(entity_for_topic(first["kafka_topic"]), datetime.now(timezone.utc).date(),
                    first["kafka_partition"], first["kafka_offset"], last["kafka_offset"])
    body = "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in records)
    s3io.client().put_object(Bucket=load_settings().s3_bucket, Key=key, Body=body.encode("utf-8"))
    return key


def flush(consumer: Consumer, buffer: BatchBuffer, keys: list[tuple[str, int]]) -> None:
    """Записать готовые пачки и только после этого зафиксировать их offset."""
    for topic, partition in keys:
        records = buffer.drain(topic, partition)
        key = write_batch(records)
        # «Offset фиксируется только после успешной записи файла»: если write_batch упадёт,
        # коммита не будет и сообщения перечитаются (возможен дубль, но не потеря).
        # Коммитим «следующий к чтению» offset.
        next_offset = records[-1]["kafka_offset"] + 1
        consumer.commit(offsets=[TopicPartition(topic, partition, next_offset)], asynchronous=False)
        log.info("archived %d messages -> %s", len(records), key)


def make_consumer(buffer: BatchBuffer) -> Consumer:
    """Consumer своей группы; новая группа читает топики с начала."""
    consumer = Consumer({
        "bootstrap.servers": load_settings().kafka_bootstrap,
        "group.id": GROUP_ID,                # «своя consumer group»: независимо от ClickHouse
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,         # коммитим сами, только после записи в S3 (см. flush)
    })

    def on_revoke(_consumer, partitions):
        """Партиции ушли другому участнику группы - их незаписанные пачки не коммитим."""
        buffer.drop([(p.topic, p.partition) for p in partitions])

    consumer.subscribe(list(TOPICS), on_revoke=on_revoke)   # «читает оба топика»
    return consumer


def accept(message, buffer: BatchBuffer, now: float) -> None:
    """Положить сообщение в буфер; не-JSON пропускается с записью в лог, а не роняет сервис."""
    try:
        record = envelope(message.value(), message.topic(), message.partition(),
                          message.offset(), datetime.now(timezone.utc))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        log.warning("skip non-JSON message %s[%d]@%d: %s", message.topic(), message.partition(),
                    message.offset(), exc)
        return
    buffer.add(message.topic(), message.partition(), record, now)


def run() -> None:
    """
    Архиватор Kafka -> S3.

    Основной цикл: читать, копить, сбрасывать готовые пачки."""
    buffer = BatchBuffer()
    consumer = make_consumer(buffer)
    try:
        while True:
            # Читает оба топика своей consumer group
            message = consumer.poll(1.0)
            now = time.monotonic()
            if message is not None:
                if message.error():
                    raise KafkaException(message.error())
                accept(message, buffer, now)       # «копит сообщения пачками»
            flush(consumer, buffer, buffer.ready(now))   # готовые пачки -> файл в S3 -> коммит offset
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
