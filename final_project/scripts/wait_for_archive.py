from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from cinema_dwh import s3io
from cinema_dwh.config import load_settings
from cinema_dwh.paths import RAW_EVENTS_PREFIX, RAW_PROFILES_PREFIX

SOURCES = {RAW_EVENTS_PREFIX: ("events.jsonl", "event_id"), RAW_PROFILES_PREFIX: ("profile_changes.jsonl", "change_id")}


def expected_ids(path: Path, id_field: str, until: str | None) -> set[str]:
    """ID сообщений набора, уже отправленных в Kafka (available_at < until)."""
    with path.open(encoding="utf-8") as source:
        rows = [json.loads(line) for line in source if line.strip()]
    return {row[id_field] for row in rows if until is None or row["available_at"] < until}


def archived_ids(prefix: str, id_field: str) -> set[str]:
    """ID, которые сейчас лежат в архиве под префиксом."""
    client, bucket = s3io.client(), load_settings().s3_bucket
    ids = set()
    for key in s3io.list_keys(prefix):
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        ids.update(json.loads(line)[id_field] for line in body.splitlines() if line.strip())
    return ids


def main() -> int:
    """
    Дождаться, пока архиватор сохранит в S3 все отправленные сообщения набора.

    Код выхода 0 - архив догнал отправку.
    """
    parser = argparse.ArgumentParser(description="Дождаться, пока архиватор сохранит в S3 все отправленные сообщения")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--until-arrival")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    want = {prefix: expected_ids(args.dataset / name, field, args.until_arrival)
            for prefix, (name, field) in SOURCES.items()}
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        # сравниваем уникальные ID, а не строки: дубль архиватора не должен закончить ожидание раньше времени
        missing = {prefix: len(ids - archived_ids(prefix, SOURCES[prefix][1]))
                   for prefix, ids in want.items()}

        print(f"archive: missing unique ids {missing} of {({p: len(i) for p, i in want.items()})}", flush=True)
        if not any(missing.values()):
            return 0
        time.sleep(5)
    return 1


if __name__ == "__main__":
    sys.exit(main())
