from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone

import clickhouse_connect

from cinema_dwh.config import load_settings
from cinema_dwh.run_context import interval_days

DB = "cinema"   # база ClickHouse со всеми таблицами проекта
# Колонки batch_runs в порядке DDL (clickhouse/ddl/30_batch_runs.sql)
RUN_COLUMNS = ["run_id",
               "airflow_run_id",
               "interval_start",
               "interval_end",
               "snapshot_created_at",
               "affected_dates",
               "affected_reasons",
               "status",
               "published_at",
               "started_at",
               "updated_at"]
TIME_COLUMNS = ("interval_start",
                "interval_end",
                "snapshot_created_at",
                "published_at",
                "started_at",
                "updated_at")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)   # «пустое» время: так ClickHouse отвечает max() по пустой таблице


class StaleSnapshotError(Exception):
    """Срез этого запуска старше уже опубликованного - публикация вернула бы старые цифры (откат)."""


class PublishError(Exception):
    """ClickHouse загрузил из S3 не столько строк, сколько лежит в Parquet-файлах витрины."""


def client():
    """Подключение к ClickHouse по HTTP (адрес и логин - из переменных CLICKHOUSE_*)."""
    s = load_settings()
    return clickhouse_connect.get_client(host=s.clickhouse_host, port=s.clickhouse_port,
                                         username=s.clickhouse_user, password=s.clickhouse_password,
                                         database=DB)


def _utc(moment: datetime | None) -> datetime | None:
    """ClickHouse отдаёт время без часового пояса (хотя оно в UTC) - явно помечаем его как UTC."""
    if moment is None:
        return None
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment.astimezone(timezone.utc)


def _row_to_dict(names, row) -> dict:
    """Строка ответа ClickHouse (кортеж значений) -> словарь {колонка: значение}, время в UTC."""
    record = dict(zip(names, row))
    for key in TIME_COLUMNS:
        record[key] = _utc(record.get(key))
    return record


# Журнал запусков: таблица batch_runs (одна строка на запуск DAG)

def latest_run(ch, run_id: str) -> dict | None:
    """Строка запуска run_id из batch_runs (самая свежая её версия) или None, если запуска ещё нет."""
    # FINAL: ReplacingMergeTree хранит несколько версий строки - FINAL оставляет последнюю
    result = ch.query(f"SELECT {', '.join(RUN_COLUMNS)} "
                      f"FROM {DB}.batch_runs FINAL "
                      f"WHERE run_id = {{rid:String}}",
                      parameters={"rid": run_id})
    return _row_to_dict(result.column_names, result.result_rows[0]) if result.result_rows else None


def record_run(ch, run_id: str, **changes) -> dict:
    """Обновить поля запуска в batch_runs, например record_run(ch, run_id, status="failed").

    UPDATE в ClickHouse нет, поэтому: читаем текущую строку, меняем нужные поля и вставляем новую
    полную строку с более поздним updated_at - она и считается актуальной.
    """
    now = datetime.now(timezone.utc)
    # если запуска ещё нет - начинаем с пустой строки
    row = latest_run(ch, run_id) or {"run_id": run_id,
                                     "airflow_run_id": "",
                                     "interval_start": EPOCH,
                                      "interval_end": EPOCH,
                                     "snapshot_created_at": None,
                                     "affected_dates": [],
                                      "affected_reasons": "[]",
                                     "status": "running",
                                     "published_at": None,
                                      "started_at": now}
    row.update(changes)
    row["updated_at"] = now   # версия строки для ReplacingMergeTree
    if not isinstance(row["affected_reasons"], str):   # в таблице это JSON-строка
        row["affected_reasons"] = json.dumps(row["affected_reasons"], ensure_ascii=False)
    ch.insert(f"{DB}.batch_runs", [[row[c] for c in RUN_COLUMNS]], column_names=RUN_COLUMNS)
    return row


def published_runs(ch) -> list[dict]:
    """Все запуски, результаты которых хоть раз попали в ClickHouse (published_at заполнен).

    Смотрим на published_at, а не на status: неудачный перезапуск ставит status=failed,
    но данные прошлой публикации этого запуска остаются в отчёте.
    """
    result = ch.query(f"SELECT {', '.join(RUN_COLUMNS)} FROM {DB}.batch_runs FINAL WHERE published_at IS NOT NULL")
    return [_row_to_dict(result.column_names, row) for row in result.result_rows]


def mark_published(ch, run_id: str) -> None:
    """Отметить запуск опубликованным: status=published; published_at ставится только в первый раз."""
    row = latest_run(ch, run_id)
    record_run(ch, run_id, status="published",
               published_at=(row or {}).get("published_at") or datetime.now(timezone.utc))


# чистая логика без запросов (published_days, next_boundary, assert_snapshot_not_stale) - проверяется тестами без базы
def published_days(runs: list[dict]) -> set[date]:
    """Какие сутки уже лежат в batch-таблицах: сутки интервалов и пересчитанные сутки всех опубликованных запусков."""
    days = set()
    for run in runs:
        days.update(interval_days(run["interval_start"], run["interval_end"]))
        days.update(run["affected_dates"])
    return days


def assert_snapshot_not_stale(snapshot_created_at: datetime, runs: list[dict]) -> None:
    """Защита от отката: упасть, если срез этого запуска старше среза самой свежей публикации.

    Повтор того же запуска проходит (срез тот же - время совпадает). Вызывается дважды:
    сразу после фиксации среза (не тратить расчёт впустую) и перед публикацией
    (вдруг за время расчёта опубликовался запуск посвежее).
    """
    newest = max((r["snapshot_created_at"] for r in runs if r.get("snapshot_created_at")), default=None)
    if newest is not None and snapshot_created_at < newest:
        raise StaleSnapshotError(
            f"срез устарел: {snapshot_created_at:%Y-%m-%d %H:%M:%S} старше опубликованного {newest:%Y-%m-%d %H:%M:%S}")


# Граница T: до T общий отчёт берёт batch, от T - speed

def next_boundary(current_t: datetime | None, days: set[date], batch_start: date) -> datetime | None:
    """Куда сдвинуть T: от текущей T (или от 1 января) шагаем по суткам вперёд, пока сутки опубликованы.

    Пример: T = 2 янв, опубликованы {1, 2, 3, 5 янв} -> T = 4 янв (4 января нет - на нём цепочка рвётся).
    T никогда не уменьшается; если 1 января не опубликовано - T не появляется вовсе.
    """
    day = current_t.date() if current_t else batch_start
    while day in days:
        day += timedelta(days=1)
    candidate = datetime.combine(day, time(0, 0), tzinfo=timezone.utc)   # полночь первого неопубликованного дня
    if current_t is None:
        return candidate if day > batch_start else None
    return max(current_t, candidate)


def current_boundary(ch) -> datetime | None:
    """Текущая T из таблицы serving_boundary или None, если её ещё ни разу не выставляли."""
    value = _utc(ch.query(f"SELECT max(boundary_t) FROM {DB}.serving_boundary").result_rows[0][0])
    return None if value is None or value <= EPOCH else value


def advance_boundary(ch, run_id: str, batch_start: date) -> datetime | None:
    """Пересчитать T и, если она выросла, записать новую строку в serving_boundary; вернуть текущую T."""
    current = current_boundary(ch)
    new = next_boundary(current, published_days(published_runs(ch)), batch_start)
    if new is not None and (current is None or new > current):
        ch.insert(f"{DB}.serving_boundary", [[new, run_id, datetime.now(timezone.utc)]],
                  column_names=["boundary_t", "run_id", "updated_at"])
    return new


# Публикация витрин: Parquet из S3 -> временная таблица -> подмена суток в batch_*

def _columns(ch, table: str) -> list[str]:
    """Имена колонок таблицы ClickHouse в порядке DDL (они же - имена колонок в Parquet витрины)."""
    result = ch.query("SELECT name "
                      "FROM system.columns "
                      "WHERE database = {db:String} AND table = {t:String} "
                      "ORDER BY position", parameters={"db": DB, "t": table})
    return [row[0] for row in result.result_rows]


def _partitions(ch, table: str) -> set[str]:
    """За какие сутки в таблице есть данные: {"2026-01-01", ...} (партиция batch-таблицы = одни сутки)."""
    result = ch.query("SELECT "
                      "DISTINCT partition "
                      "FROM system.parts "
                      "WHERE database = {db:String} AND table = {t:String} AND active",
                      parameters={"db": DB, "t": table})
    return {row[0] for row in result.result_rows}


def _fill_staging(ch, table: str, staging: str, s3_glob: str, expected_rows: int) -> None:
    """Создать временную таблицу staging (копию структуры table) и загрузить в неё Parquet витрины из S3."""
    ch.command(f"DROP TABLE IF EXISTS {DB}.{staging}")       # остаток упавшего запуска
    ch.command(f"CREATE TABLE {DB}.{staging} AS {DB}.{table}")   # те же колонки и партиции, но пустая
    if expected_rows == 0:                                   # пустая витрина: Spark не пишет part-файлов
        return
    cols = ", ".join(_columns(ch, table))
    # s3(cinema_s3, ...) - ClickHouse сам читает файлы из бакета (доступ - docker/clickhouse/config.d)
    ch.command(f"INSERT INTO {DB}.{staging} ({cols}) SELECT {cols} "
               f"FROM s3(cinema_s3, filename = '{s3_glob}', format = 'Parquet')")
    loaded = ch.query(f"SELECT count() FROM {DB}.{staging}").result_rows[0][0]
    if loaded != expected_rows:   # недогрузилось - публиковать нельзя
        raise PublishError(f"{table}: в staging {loaded} строк, Spark посчитал {expected_rows}")


def parquet_rows(ch, s3_glob: str, has_files: bool) -> int:
    """Сколько строк в Parquet-файлах витрины в S3 (s3_glob - маска вида batch/<run_id>/marts/views_minute/*.parquet)."""
    if not has_files:   # s3() по маске без файлов падает - пустая витрина = 0 строк
        return 0
    return ch.query(f"SELECT count() "
                    f"FROM s3(cinema_s3, filename = '{s3_glob}', format = 'Parquet')").result_rows[0][0]


def publish_mart(ch, table: str, s3_glob: str, affected: list[date], expected_rows: int) -> None:
    """Заменить в таблице table данные за сутки affected на данные витрины из S3 (повтор не создаёт дублей).

    Каждые сутки подменяются атомарно (REPLACE PARTITION): читатель видит либо старый день, либо новый.
    Если запуск упадёт между сутками, повтор задачи подменит их заново - результат тот же.
    """
    staging = f"stg_{table}"
    _fill_staging(ch, table, staging, s3_glob, expected_rows)
    present = _partitions(ch, staging)   # за какие сутки витрина реально что-то посчитала
    for day in sorted(affected):
        if day.isoformat() in present:
            # REPLACE PARTITION: сутки day в table целиком заменяются сутками day из staging
            ch.command(f"ALTER TABLE {DB}.{table} REPLACE PARTITION '{day.isoformat()}' FROM {DB}.{staging}")
        else:  # день опустел - убираем старые строки
            ch.command(f"ALTER TABLE {DB}.{table} DROP PARTITION '{day.isoformat()}'")
    ch.command(f"DROP TABLE {DB}.{staging}")   # временная таблица больше не нужна
