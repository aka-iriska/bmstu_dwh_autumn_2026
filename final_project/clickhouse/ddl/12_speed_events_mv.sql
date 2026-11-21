CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.speed_events_mv
TO cinema.speed_views
AS
WITH
    -- разбирает json
    JSONExtract(raw, 'content_id', 'Nullable(UInt32)')                AS content_id_n,
    JSONExtract(raw, 'watch_time_seconds', 'Nullable(UInt32)')        AS watch_n,
    JSONExtract(raw, 'user_id', 'Nullable(UInt32)')                   AS user_id_n,
    -- плохое время не роняет чтение топика
    parseDateTimeBestEffortOrNull(JSONExtractString(raw, 'event_time'), 'UTC') AS event_time_n
SELECT
    JSONExtractString(raw, 'event_id') AS event_id,
    assumeNotNull(event_time_n)        AS event_time,
    assumeNotNull(user_id_n)           AS user_id,
    assumeNotNull(content_id_n)        AS content_id,
    assumeNotNull(watch_n)             AS watch_time_seconds,
    toInt32(_partition)                AS kafka_partition,
    toInt64(_offset)                   AS kafka_offset
FROM cinema.speed_events_queue
WHERE _error = ''                                   -- сообщения с ошибкой разбора не роняют поток
  AND JSONExtractString(raw, 'event_type') = 'view'
  AND JSONExtractString(raw, 'event_id') != ''
  AND event_time_n IS NOT NULL AND user_id_n IS NOT NULL
  AND content_id_n IS NOT NULL AND watch_n IS NOT NULL;
