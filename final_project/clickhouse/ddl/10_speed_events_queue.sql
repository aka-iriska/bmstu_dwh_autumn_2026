-- каждое сообщение один раз отдаётся материализованному представлению speed_events_mv.
CREATE TABLE IF NOT EXISTS cinema.speed_events_queue
(
    raw String -- JSONAsString - строка целиком в одну колонку, разбор в MV (явно обрабатываем null и content_id = 0).
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'cinema.events',
    kafka_group_name = 'cinema-clickhouse-speed',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    -- Битое сообщение не останавливает чтение: ошибка попадает в виртуальную колонку _error.
    kafka_handle_error_mode = 'stream';
