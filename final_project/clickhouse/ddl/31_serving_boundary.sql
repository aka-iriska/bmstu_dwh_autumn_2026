-- serving_boundary - граница T общего отчёта:
CREATE TABLE IF NOT EXISTS cinema.serving_boundary
(
    boundary_t DateTime('UTC'), -- до T показатели берутся из batch, от T - из speed.
    run_id     String,          -- какой запуск сдвинул границу (batch_runs.run_id)
    updated_at DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY updated_at;
