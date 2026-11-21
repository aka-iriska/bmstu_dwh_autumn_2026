-- Повторная доставка: сколько раз каждый event_id лежит в сыром архиве S3 (повторы видны)
-- и сколько раз он учтён в оперативном слое (speed_views FINAL) - должен быть ровно один.
SELECT raw.event_id, raw.copies_in_archive, speed.rows_in_speed_final
FROM
(
    SELECT JSONExtractString(json, 'event_id') AS event_id, count() AS copies_in_archive
    FROM s3(cinema_s3, filename = 'raw/events/**/*.jsonl', format = 'JSONAsString')
    WHERE JSONExtractString(json, 'event_type') = 'view'
    GROUP BY event_id
    HAVING copies_in_archive > 1
) AS raw
LEFT JOIN
(
    SELECT event_id, count() AS rows_in_speed_final FROM cinema.speed_views FINAL GROUP BY event_id
) AS speed USING (event_id)
ORDER BY raw.event_id
LIMIT 20;
