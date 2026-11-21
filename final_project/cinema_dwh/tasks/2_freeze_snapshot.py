from __future__ import annotations

from cinema_dwh import clickhouse, snapshot
from cinema_dwh.run_context import from_iso
from cinema_dwh.tasks.context import log_summary, read_snapshot, run_meta


def published_file_lists(runs: list[dict]) -> list[list[str]]:
    """Списки файлов из срезов опубликованных запусков."""
    lists = []
    for run in runs:
        files = read_snapshot(run["run_id"])["files"]
        lists.append(files["events"] + files["profile_changes"])
    return lists


def run(**context) -> None:
    """Записать snapshot.json (повтор задачи переиспользует уже записанный) и время среза в журнал.

    Срез старше самой свежей публикации сразу останавливает запуск: дальше считать бессмысленно,
    а публиковать - нельзя (откатило бы свежие данные).
    """
    meta, ch = run_meta(context), clickhouse.client()
    others = [r for r in clickhouse.published_runs(ch) if r["run_id"] != meta.run_id]
    snap = snapshot.freeze(meta, published_file_lists(others))
    created_at = from_iso(snap["created_at"])
    clickhouse.assert_snapshot_not_stale(created_at, others)
    clickhouse.record_run(ch, meta.run_id, snapshot_created_at=created_at)
    log_summary("Срез", {"created_at": snap["created_at"], "event_files": len(snap["files"]["events"]),
                         "profile_files": len(snap["files"]["profile_changes"]), "new_files": len(snap["new_files"])})
