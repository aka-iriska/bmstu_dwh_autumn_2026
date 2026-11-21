-- batch_top_content_daily - дневной топ-5 контента: по числу просмотров, при равенстве - по content_id.
CREATE TABLE IF NOT EXISTS cinema.batch_top_content_daily
(
    event_date     Date,
    rank           UInt8,
    content_id     UInt32,
    category       LowCardinality(String),
    views_count    UInt64,
    run_id         String,
    interval_start DateTime('UTC'),
    interval_end   DateTime('UTC'),
    processed_at   DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, rank);
