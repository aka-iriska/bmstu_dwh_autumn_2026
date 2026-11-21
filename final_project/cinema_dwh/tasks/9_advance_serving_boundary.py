from __future__ import annotations

from cinema_dwh import clickhouse
from cinema_dwh.config import BATCH_START
from cinema_dwh.tasks.context import log_summary, run_meta


def run(**context) -> None:
    """T идёт вперёд по непрерывной цепочке опубликованных суток, начиная с BATCH_START."""
    meta, ch = run_meta(context), clickhouse.client()
    before = clickhouse.current_boundary(ch)
    after = clickhouse.advance_boundary(ch, meta.run_id, BATCH_START)
    log_summary("Граница T", {"было": before, "стало": after})
