-- SCD2-история профилей из S3 (Parquet последнего опубликованного запуска), без копии в ClickHouse.
-- Запуск: clickhouse-client --param_run_id=<run_id> --queries-file clickhouse/queries/07_scd2_user.sql
SELECT user_id, version, country, subscription_tier, valid_from, valid_to, is_current, source_change_id
FROM s3(cinema_s3, filename = concat('batch/', {run_id:String}, '/user_profile_scd2/*.parquet'), format = 'Parquet')
ORDER BY user_id, valid_from;
