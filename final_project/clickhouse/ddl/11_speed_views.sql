-- сырые просмотры оперативного контура (целевая таблица MV).
CREATE TABLE IF NOT EXISTS cinema.speed_views
(
    event_id           String,
    event_time         DateTime('UTC'),
    user_id            UInt32,
    content_id         UInt32,                       -- 0 - валидный ID
    watch_time_seconds UInt32,
    kafka_partition    Int32,
    kafka_offset       Int64,
    ingested_at        DateTime('UTC') DEFAULT now() -- версия для ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(ingested_at)            -- схлопывает строки с одинаковым ключом сортировки
PARTITION BY toDate(event_time)                     -- повторы одного события всегда в одной партиции (иначе не схлопнутся)
ORDER BY (toStartOfMinute(event_time), content_id, event_id);
