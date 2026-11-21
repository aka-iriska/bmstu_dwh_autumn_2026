-- Общий отчёт «минута × контент» (первые 20 строк): детализация и метрики одинаковы в обоих слоях.
SELECT minute_start, content_id, views_count, watch_time_seconds_sum, source
FROM cinema.report_views_minute
ORDER BY minute_start, content_id
LIMIT 20;
