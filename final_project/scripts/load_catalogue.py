from __future__ import annotations

import argparse
from pathlib import Path

from botocore.exceptions import ClientError

from cinema_dwh import s3io
from cinema_dwh.config import load_settings
from cinema_dwh.paths import CATALOGUE_KEY

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"


def ensure_bucket(client, bucket: str) -> None:
    """Создать бакет, если его ещё нет."""
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        client.create_bucket(Bucket=bucket)


def main() -> None:
    """Создаёт бакет проекта и кладёт в него каталог выбранного набора."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("fixture", "demo"), required=True)
    args = parser.parse_args()

    bucket = load_settings().s3_bucket
    client = s3io.client()
    ensure_bucket(client, bucket)
    source = DATA_ROOT / args.dataset / "catalogue.jsonl"
    client.upload_file(str(source), bucket, CATALOGUE_KEY)
    print(f"UPLOADED {source.name} ({args.dataset}) -> s3://{bucket}/{CATALOGUE_KEY}", flush=True)


if __name__ == "__main__":
    main()
