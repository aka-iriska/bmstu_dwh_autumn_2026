-- speed_views_minute - оперативные показатели «минута × контент».
CREATE VIEW IF NOT EXISTS cinema.speed_views_minute
AS
SELECT
    -- Те же колонки, что у batch_views_minute.
    toStartOfMinute(event_time) AS minute_start,
    content_id,
    count()                     AS views_count,
    sum(watch_time_seconds)     AS watch_time_seconds_sum
FROM cinema.speed_views FINAL
GROUP BY minute_start, content_id;
