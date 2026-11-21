from __future__ import annotations

from cinema_dwh import clickhouse, snapshot
from cinema_dwh.run_context import from_iso
from cinema_dwh.tasks.context import log_summary, read_snapshot, run_meta

# Шаг 2 DAG: «заморозить» список сырых файлов в S3, с которыми работает этот запуск.
# Читает: список файлов raw/ в S3, журнал batch_runs.
# Пишет: batch/<run_id>/snapshot.json.
# Зачем: архиватор продолжает дописывать файлы, а расчёт должен видеть один и тот же набор от начала до конца.


def published_file_lists(runs: list[dict]) -> list[list[str]]:
    """Для каждого опубликованного запуска - список сырых файлов из его snapshot.json."""
    lists = []
    for run in runs:
        files = read_snapshot(run_id=run["run_id"])["files"]
        lists.append(files["events"] + files["profile_changes"])
    return lists


def run(**context) -> None:
    """Записать snapshot.json (повтор задачи переиспользует уже записанный) и время среза в журнал.

    Срез старше самой свежей публикации сразу останавливает запуск: дальше считать бессмысленно,
    а публиковать - нельзя (откатило бы свежие данные).
    """
    meta = run_meta(context=context)
    ch = clickhouse.client()
    # Другие опубликованные запуски.
    all_published = clickhouse.published_runs(ch=ch)
    others = [r for r in all_published if r["run_id"] != meta.run_id]
    # snapshot.json = все сырые файлы сейчас + new_files: те, которых нет ни в одном опубликованном срезе.
    # По new_files шаг 3 находит поздние данные.
    already_seen = published_file_lists(runs=others)
    snap = snapshot.freeze(meta=meta, published_file_lists=already_seen)
    created_at = from_iso(text=snap["created_at"])
    # Защита от отката: если кто-то уже опубликовал более свежий срез - падаем.
    clickhouse.assert_snapshot_not_stale(snapshot_created_at=created_at, runs=others)
    # Время среза кладём в журнал: шаг 8 проверит его ещё раз перед публикацией.
    clickhouse.record_run(ch=ch, run_id=meta.run_id, snapshot_created_at=created_at)
    summary = {"created_at": snap["created_at"],
               "event_files": len(snap["files"]["events"]),
               "profile_files": len(snap["files"]["profile_changes"]),
               "new_files": len(snap["new_files"])}
    log_summary(title="Срез", values=summary)
