from __future__ import annotations

from cinema_dwh import clickhouse
from cinema_dwh.config import BATCH_START
from cinema_dwh.tasks.context import log_summary, run_meta

# Шаг 9 DAG: сдвинуть границу T общего отчёта report_views_minute.
# До T отчёт берёт минуты из batch_views_minute, от T - из speed_views_minute.
# Читает: журнал batch_runs.
# Пишет: новую строку в cinema.serving_boundary.


def run(**context) -> None:
    """T идёт вперёд по непрерывной цепочке опубликованных суток, начиная с BATCH_START."""
    meta = run_meta(context=context)
    ch = clickhouse.client()
    before = clickhouse.current_boundary(ch=ch)
    after = clickhouse.advance_boundary(ch=ch, run_id=meta.run_id, batch_start=BATCH_START)
    log_summary(title="Граница T", values={"было": before, "стало": after})
