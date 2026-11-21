from __future__ import annotations

from datetime import datetime, timezone

from cinema_dwh import clickhouse
from cinema_dwh.run_context import from_iso, make_run_id, to_iso, validate_interval
from cinema_dwh.tasks.context import log_summary

# Шаг 1 DAG: решить, какой интервал считаем, выдать запуску run_id и записать его в журнал batch_runs.
# Читает: параметры Airflow.
# Пишет: строку в cinema.batch_runs (status=running), run_context в XCom.


def _resolve_interval(params: dict, data_interval_start: datetime, data_interval_end: datetime) -> tuple:
    """Интервал из параметров запуска (пересчёт периода руками) или из расписания."""
    # Если при ручном запуске передали interval_start / interval_end - берём их,
    # иначе - сутки, которые Airflow назначил по расписанию (data_interval).
    if params.get("interval_start"):
        start = from_iso(text=params["interval_start"])
    else:
        start = data_interval_start
    if params.get("interval_end"):
        end = from_iso(text=params["interval_end"])
    else:
        end = data_interval_end
    # Всё храним в UTC, чтобы сутки резались одинаково везде (Spark, ClickHouse, S3).
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    # Обе границы - полночь UTC, начало раньше конца, всё внутри января 2026 - иначе падаем сразу.
    validate_interval(start=start, end=end)
    return start, end


def run(**context) -> dict:
    """Вернуть run_context (XCom) и записать запуск в журнал."""
    airflow_run_id = context["run_id"]
    start, end = _resolve_interval(params=context["params"],
                                   data_interval_start=context["data_interval_start"],
                                   data_interval_end=context["data_interval_end"])
    # run_id = дата + хеш (airflow run_id, интервал): у каждого запуска своя папка batch/<run_id>/ в S3.
    run_id = make_run_id(interval_start=start, interval_end=end, airflow_run_id=airflow_run_id)
    # Журнал: запуск начался. Статус потом сменится на published (шаг 8) или failed.
    ch = clickhouse.client()
    clickhouse.record_run(ch=ch, run_id=run_id, airflow_run_id=airflow_run_id,
                          interval_start=start, interval_end=end, status="running")
    # Этот словарь уходит в XCom - из него все следующие шаги узнают run_id и интервал.
    run_context = {"run_id": run_id,
                   "airflow_run_id": airflow_run_id,
                   "interval_start": to_iso(moment=start),
                   "interval_end": to_iso(moment=end)}
    log_summary(title="Контекст запуска", values=run_context)
    return run_context
