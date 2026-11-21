from __future__ import annotations

from cinema_dwh import clickhouse, s3io
from cinema_dwh.paths import MARTS, mart_key
from cinema_dwh.tasks.context import affected, log_summary, run_meta

# Шаг 8 DAG: залить витрины из S3 в batch-таблицы ClickHouse.
# Заменяются только затронутые сутки (affected_dates), остальные дни в таблицах не трогаются.
# Читает: batch/<run_id>/marts/<витрина>/*.parquet.
# Пишет: cinema.batch_<витрина>, статус published в batch_runs.


def run(**context) -> None:
    """Подменить затронутые сутки batch-таблиц; запуск получает статус published."""
    meta = run_meta(context=context)
    days = affected(context=context)
    ch = clickhouse.client()
    # Второй раз (первый - в freeze_snapshot): вдруг за время расчёта опубликовался более свежий срез.
    this_run = clickhouse.latest_run(ch=ch, run_id=meta.run_id)
    all_published = clickhouse.published_runs(ch=ch)
    others = [r for r in all_published if r["run_id"] != meta.run_id]
    clickhouse.assert_snapshot_not_stale(snapshot_created_at=this_run["snapshot_created_at"], runs=others)
    loaded = {}
    for name in MARTS:
        prefix = mart_key(run_id=meta.run_id, mart=name)
        # Есть ли в папке витрины хоть один .parquet. Если витрина пустая, файла может не быть,
        # а функция s3() в ClickHouse на пустом шаблоне падает - тогда просто считаем 0 строк.
        keys = s3io.list_keys(prefix=prefix)
        has_files = any(key.endswith(".parquet") for key in keys)
        # Шаблон пути для функции s3() в ClickHouse: все parquet-файлы этой витрины.
        glob = f"{prefix}*.parquet"
        loaded[name] = clickhouse.parquet_rows(ch=ch, s3_glob=glob, has_files=has_files)
        # По каждому затронутому дню: залить в stg_<таблица>, затем REPLACE PARTITION в batch_<таблица>.
        # Повторный запуск даёт тот же результат, без дублей.
        clickhouse.publish_mart(ch=ch, table=f"batch_{name}", s3_glob=glob,
                                affected=days, expected_rows=loaded[name])
    # Только после всех витрин: status=published и published_at. По ним шаг 9 двигает T.
    clickhouse.mark_published(ch=ch, run_id=meta.run_id)
    day_strings = [d.isoformat() for d in days]
    log_summary(title="Опубликовано (строк)", values={**loaded, "days": day_strings})
