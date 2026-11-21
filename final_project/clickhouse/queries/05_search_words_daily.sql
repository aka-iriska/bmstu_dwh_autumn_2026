-- Частоты поисковых слов по суткам (первые 10 слов каждого дня по частоте).
SELECT event_date, word, word_count
FROM cinema.batch_search_words_daily
ORDER BY event_date, word_count DESC, word
LIMIT 10 BY event_date;
