from __future__ import annotations

from datetime import datetime, timezone

from cinema_dwh import clickhouse
from cinema_dwh.run_context import from_iso, make_run_id, to_iso, validate_interval
from cinema_dwh.tasks.context import log_summary


def _resolve_interval(params: dict, data_interval_start: datetime, data_interval_end: datetime) -> tuple:
    """Интервал из параметров запуска (пересчёт периода руками) или из расписания."""
    start = from_iso(params["interval_start"]) if params.get("interval_start") else data_interval_start
    end = from_iso(params["interval_end"]) if params.get("interval_end") else data_interval_end
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    validate_interval(start, end)
    return start, end


def run(**context) -> dict:
    """Вернуть run_context (XCom) и записать запуск в журнал."""
    start, end = _resolve_interval(context["params"], context["data_interval_start"], context["data_interval_end"])
    run_id = make_run_id(start, end, context["run_id"])
    clickhouse.record_run(clickhouse.client(), run_id, airflow_run_id=context["run_id"],
                          interval_start=start, interval_end=end, status="running")
    run_context = {"run_id": run_id,
                   "airflow_run_id": context["run_id"],
                   "interval_start": to_iso(start),
                   "interval_end": to_iso(end)}
    log_summary("Контекст запуска", run_context)
    return run_context
