-- batch_views_country_tier_daily - исторический отчёт
CREATE TABLE IF NOT EXISTS cinema.batch_views_country_tier_daily
(
    event_date             Date,
    country                LowCardinality(String),
    subscription_tier      LowCardinality(String),
    views_count            UInt64,
    watch_time_seconds_sum UInt64,
    run_id                 String,
    interval_start         DateTime('UTC'),
    interval_end           DateTime('UTC'),
    processed_at           DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, country, subscription_tier);
