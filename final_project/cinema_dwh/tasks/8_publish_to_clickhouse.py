from __future__ import annotations

from cinema_dwh import clickhouse, s3io
from cinema_dwh.paths import MARTS, mart_key
from cinema_dwh.tasks.context import affected, log_summary, run_meta


def run(**context) -> None:
    """Подменить затронутые сутки batch-таблиц; запуск получает статус published."""
    meta, days, ch = run_meta(context), affected(context), clickhouse.client()
    this_run = clickhouse.latest_run(ch, meta.run_id)
    # Второй раз (первый - в freeze_snapshot): вдруг за время расчёта опубликовался более свежий срез.
    others = [r for r in clickhouse.published_runs(ch) if r["run_id"] != meta.run_id]
    clickhouse.assert_snapshot_not_stale(this_run["snapshot_created_at"], others)
    loaded = {}
    for name in MARTS:
        prefix = mart_key(meta.run_id, name)
        has_files = any(key.endswith(".parquet") for key in s3io.list_keys(prefix))
        glob = f"{prefix}*.parquet"
        loaded[name] = clickhouse.parquet_rows(ch, glob, has_files)
        clickhouse.publish_mart(ch, f"batch_{name}", glob, days, loaded[name])
    clickhouse.mark_published(ch, meta.run_id)
    log_summary("Опубликовано (строк)", {**loaded, "days": [d.isoformat() for d in days]})
