-- Исторический JOIN на границе версии профиля: просмотры пользователя 1001 около 12:00 1 января
-- и версия профиля, к которой каждый отнесён (интервал версии [valid_from, valid_to): 12:00:00 - уже новая).
-- Читается прямо из S3 (views_enriched запуска).
-- Запуск: clickhouse-client --param_run_id=<run_id> --queries-file clickhouse/queries/08_views_enriched_boundary.sql
SELECT event_id, event_time, content_id, country, subscription_tier, profile_version
FROM s3(cinema_s3, filename = concat('batch/', {run_id:String}, '/views_enriched/**/*.parquet'), format = 'Parquet')
WHERE user_id = 1001 AND event_time BETWEEN '2026-01-01 11:00:00' AND '2026-01-01 13:00:00'
ORDER BY event_time;
