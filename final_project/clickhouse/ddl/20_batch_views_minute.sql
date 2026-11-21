-- batch_views_minute - пакетные показатели «минута × контент» (те же колонки, что speed_views_minute).
CREATE TABLE IF NOT EXISTS cinema.batch_views_minute
(
    minute_start           DateTime('UTC'),
    content_id             UInt32,
    views_count            UInt64,
    watch_time_seconds_sum UInt64,
    run_id                 String,           -- метаполя: каким запуском посчитано
    interval_start         DateTime('UTC'),
    interval_end           DateTime('UTC'),
    processed_at           DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY toDate(minute_start)
ORDER BY (minute_start, content_id);
