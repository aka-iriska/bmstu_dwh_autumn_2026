-- Журнал запусков: интервал, срез, какие сутки пересчитаны и почему, статус последней попытки
-- и когда результаты запуска были опубликованы (published_at не стирается неудачным перезапуском).
SELECT run_id, interval_start, interval_end, snapshot_created_at, affected_dates, status, published_at,
       affected_reasons
FROM cinema.batch_runs FINAL
ORDER BY started_at;
