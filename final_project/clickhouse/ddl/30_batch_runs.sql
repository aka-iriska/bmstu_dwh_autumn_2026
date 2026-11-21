-- batch_runs - журнал запусков пакетного расчёта.
-- На нём держатся:
-- граница T (какие сутки опубликованы),
-- поиск поздних данных (какие срезы уже обработаны),
-- защита от отката (snapshot_created_at).
CREATE TABLE IF NOT EXISTS cinema.batch_runs
(
    run_id              String,
    airflow_run_id      String,
    interval_start      DateTime('UTC'),
    interval_end        DateTime('UTC'),
    snapshot_created_at Nullable(DateTime('UTC')),
    affected_dates      Array(Date),
    affected_reasons    String,                  -- JSON: почему каждая дата пересчитана
    status              LowCardinality(String),  -- running / published / failed (статус последней попытки)
    published_at        Nullable(DateTime64(3, 'UTC')),
    started_at          DateTime('UTC'),
    updated_at          DateTime64(3, 'UTC')     -- версия строки
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY run_id;
