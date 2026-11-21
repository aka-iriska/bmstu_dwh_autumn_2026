-- Исторический отчёт: просмотры по стране и тарифу пользователя НА ДАТУ ПРОСМОТРА.
-- run_id / interval_* показывают, каким запуском посчитана строка.
SELECT event_date, country, subscription_tier, views_count, watch_time_seconds_sum,
       run_id, interval_start, interval_end, processed_at
FROM cinema.batch_views_country_tier_daily
ORDER BY event_date, country, subscription_tier;
