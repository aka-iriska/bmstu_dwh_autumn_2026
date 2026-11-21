-- report_views_minute - общий отчёт Lambda-архитектуры «минута × контент».
CREATE VIEW IF NOT EXISTS cinema.report_views_minute
AS
WITH (SELECT max(boundary_t) FROM cinema.serving_boundary) AS t
SELECT minute_start, content_id, views_count, watch_time_seconds_sum, 'batch' AS source
FROM cinema.batch_views_minute
WHERE minute_start < t
UNION ALL  -- Время события до T берётся из пакетного слоя, от T включительно - из оперативного:
SELECT minute_start, content_id, views_count, watch_time_seconds_sum, 'speed' AS source
FROM cinema.speed_views_minute
WHERE minute_start >= t;
