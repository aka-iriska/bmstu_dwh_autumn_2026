from __future__ import annotations

from cinema_dwh import s3io
from cinema_dwh.checks import QualityCheckError, compare_rows
from cinema_dwh.paths import checks_key
from cinema_dwh.rdd.marts_rdd import search_words_rdd, top_content_rdd
from cinema_dwh.run_context import META_COLUMNS
from cinema_dwh.spark import spark_session
from cinema_dwh.tasks.context import affected, log_summary, mart_path, read_snapshot, run_meta, spark_paths


def _df_rows(spark, path: str, drop: tuple = ()) -> list[tuple]:
    """Строки витрины DataFrame-версии без метаполей."""
    df = spark.read.parquet(path)
    cols = [c for c in df.columns if c not in META_COLUMNS and c not in drop]
    return [tuple(r) for r in df.select(*cols).collect()]


def run(**context) -> None:
    """Посчитать слова и топ-5 через RDD, сравнить с витринами, записать checks/rdd_vs_df.json."""
    meta, days = run_meta(context), set(affected(context))
    files = read_snapshot(meta.run_id)["files"]["events"]
    with spark_session("compare_rdd_df") as spark:
        lines = spark.sparkContext.textFile(",".join(spark_paths(files)))
        reports = [
            compare_rows("search_words_daily", search_words_rdd(lines, days),
                         _df_rows(spark, mart_path(meta.run_id, "search_words_daily"))),
            compare_rows("top_content_daily", top_content_rdd(lines, days),
                         _df_rows(spark, mart_path(meta.run_id, "top_content_daily"), drop=("category",))),
        ]
    report = {"run_id": meta.run_id, "dates": sorted(d.isoformat() for d in days),
              "comparisons": reports, "ok": all(r["ok"] for r in reports)}
    s3io.write_json(checks_key(meta.run_id, "rdd_vs_df"), report)
    log_summary("RDD vs DataFrame", {r["name"]: f"rdd={r['rdd_rows']} df={r['df_rows']} ok={r['ok']}" for r in reports})
    if not report["ok"]:
        raise QualityCheckError("результаты RDD и DataFrame расходятся - см. checks/rdd_vs_df.json")
