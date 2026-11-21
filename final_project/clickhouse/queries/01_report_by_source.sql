-- Общий отчёт по суткам: из какого слоя взяты данные и итоги. Граница T - первой строкой.
SELECT 'T' AS day, toString(max(boundary_t)) AS source, NULL AS minutes, NULL AS views, NULL AS watch_seconds
FROM cinema.serving_boundary
UNION ALL
SELECT toString(toDate(minute_start)) AS day, source, count() AS minutes,
       sum(views_count) AS views, sum(watch_time_seconds_sum) AS watch_seconds
FROM cinema.report_views_minute
GROUP BY day, source
ORDER BY day;
