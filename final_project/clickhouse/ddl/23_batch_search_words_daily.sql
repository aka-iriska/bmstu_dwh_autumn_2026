-- batch_search_words_daily - частоты поисковых слов по суткам (уникальные поиски, слова длиннее 4 символов).
CREATE TABLE IF NOT EXISTS cinema.batch_search_words_daily
(
    event_date     Date,
    word           String,
    word_count     UInt64,
    run_id         String,
    interval_start DateTime('UTC'),
    interval_end   DateTime('UTC'),
    processed_at   DateTime('UTC')
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (event_date, word);
