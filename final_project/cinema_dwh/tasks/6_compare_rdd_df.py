from __future__ import annotations

from cinema_dwh import s3io
from cinema_dwh.checks import QualityCheckError, compare_rows
from cinema_dwh.paths import checks_key
from cinema_dwh.rdd.marts_rdd import search_words_rdd, top_content_rdd
from cinema_dwh.run_context import META_COLUMNS
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import affected, log_summary, mart_path, read_snapshot, run_meta, spark_paths

# Шаг 6 DAG (параллельно с шагом 7): посчитать две витрины второй раз через RDD и сравнить с DataFrame-версией.
# Читает: сырые файлы из snapshot.json, marts/search_words_daily и marts/top_content_daily.
# Пишет: batch/<run_id>/checks/rdd_vs_df.json. Расхождение -> задача падает, публикации не будет.


def _df_rows(spark, path: str, drop: tuple = ()) -> list[tuple]:
    """Строки DataFrame-витрины без метаполей (и без колонок из drop) - в том же виде, что выдаёт RDD."""
    df = spark.read.parquet(path)
    # Метаполей в RDD-версии нет, их сравнивать не с чем.
    cols = [c for c in df.columns if c not in META_COLUMNS and c not in drop]
    rows = df.select(*cols).collect()
    return [tuple(r) for r in rows]


def run(**context) -> None:
    """Посчитать слова и топ-5 через RDD, сравнить с витринами, записать checks/rdd_vs_df.json."""
    meta = run_meta(context=context)
    days = set(affected(context=context))
    # RDD идёт независимым путём - прямо от сырых JSON-строк, а не от clean_events.
    # Так сравнение ловит ошибки и в очистке, и в агрегации.
    event_files = read_snapshot(run_id=meta.run_id)["files"]["events"]
    words_path = mart_path(run_id=meta.run_id, mart="search_words_daily")
    top_path = mart_path(run_id=meta.run_id, mart="top_content_daily")
    with spark_session(app_name="compare_rdd_df") as spark:
        # Одна строка RDD = одна строка JSONL-файла = одно событие.
        lines = spark.sparkContext.textFile(",".join(spark_paths(keys=event_files)))
        words_report = compare_rows(name="search_words_daily",
                                    rdd_rows=search_words_rdd(lines=lines, dates=days),
                                    df_rows=_df_rows(spark=spark, path=words_path))
        # category RDD не считает (для неё нужен JOIN с каталогом) - убираем её из DataFrame-версии.
        top_report = compare_rows(name="top_content_daily",
                                  rdd_rows=top_content_rdd(lines=lines, dates=days),
                                  df_rows=_df_rows(spark=spark, path=top_path, drop=("category",)))
    reports = [words_report, top_report]
    all_ok = all(r["ok"] for r in reports)
    report = {"run_id": meta.run_id,
              "dates": sorted(d.isoformat() for d in days),
              "comparisons": reports,
              "ok": all_ok}
    # Отчёт пишем всегда, даже при расхождении - чтобы было видно, какие строки разные.
    s3io.write_json(key=checks_key(run_id=meta.run_id, name="rdd_vs_df"), obj=report)
    summary = {r["name"]: f"rdd={r['rdd_rows']} df={r['df_rows']} ok={r['ok']}" for r in reports}
    log_summary(title="RDD vs DataFrame", values=summary)
    if not all_ok:
        raise QualityCheckError("результаты RDD и DataFrame расходятся - см. checks/rdd_vs_df.json")
