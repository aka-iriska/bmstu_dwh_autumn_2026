-- Дневной топ-5 контента: по числу просмотров, при равенстве - по возрастанию content_id.
SELECT event_date, rank, content_id, category, views_count, run_id
FROM cinema.batch_top_content_daily
ORDER BY event_date, rank;
