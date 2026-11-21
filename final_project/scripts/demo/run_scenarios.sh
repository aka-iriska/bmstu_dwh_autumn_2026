#!/usr/bin/env bash
# 1) обычная обработка  2) повторный запуск  3) поздние данные и пересчёт
# 4) пересчёт исторического периода по параметрам  5) старый срез отклоняется
# 6) проваленная проверка качества останавливает публикацию.
# Внимание: в начале выполняется docker compose down -v - всё состояние стенда удаляется.
set -euo pipefail
export MSYS_NO_PATHCONV=1            # Git Bash на Windows: не переписывать пути в аргументах docker

DATASET="${1:?укажите набор: fixture или demo}"
CUTOFF="2026-01-04T00:00:00Z"        # граница двух этапов отправки (manifest.json: suggested_arrival_cutoff)
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/docs/demo/$DATASET"
cd "$ROOT"
mkdir -p "$OUT"
rm -f "$OUT"/*.txt "$OUT"/*.json

CH_BASE=(docker compose exec -T clickhouse clickhouse-client --user "${CLICKHOUSE_USER:-cinema}"
         --password "${CLICKHOUSE_PASSWORD:-cinema}")
CH=("${CH_BASE[@]}" --format PrettyCompactMonoBlock)

step() { echo; echo "=== $*"; }
tools() { docker compose run --rm tools "$@"; }
airflow() { docker compose exec -T airflow-scheduler airflow "$@"; }

# save <файл вывода> <заголовок> <sql-файл или SQL-текст> [параметры clickhouse-client]
save() {
  local out="$OUT/$1" title="$2" sql="$3"; shift 3
  { echo "-- $title"; echo; } > "$out"
  if [[ -f "$sql" ]]; then
    sed -n '/^--/p' "$sql" >> "$out"; echo >> "$out"
    "${CH[@]}" "$@" --queries-file /dev/stdin < "$sql" >> "$out"
  else
    echo "$sql" >> "$out"; echo >> "$out"
    "${CH[@]}" "$@" -q "$sql" >> "$out"
  fi
  echo "  saved $(basename "$out")"
}

# save_cmd <файл вывода> <заголовок> <команда...> - вывод произвольной команды
save_cmd() {
  local out="$OUT/$1" title="$2"; shift 2
  { echo "-- $title"; echo "-- \$ ${*}"; echo; "$@" 2>/dev/null; } > "$out" || true
  echo "  saved $(basename "$out")"
}

backfill() {  # backfill <день> [доп. флаги]: запуск DAG за сутки, как плановый
  local day="$1"; shift
  airflow dags backfill cinema_batch -s "$day" -e "$day" "$@" 2>&1 | grep -E "finished run|ERROR|срез устарел" | tail -3
}

run_id_for() {  # run_id последнего запуска с таким интервалом
  "${CH_BASE[@]}" --format TSV -q "SELECT run_id FROM cinema.batch_runs FINAL
    WHERE interval_start = '$1' AND interval_end = '$2' ORDER BY updated_at DESC LIMIT 1"
}

save_checks() {  # save_checks <run_id> <префикс файла>: отчёты проверок запуска из S3 в репозиторий
  local rid="$1" prefix="$2" name
  for name in rdd_vs_df quality; do
    tools python -c "import json; from cinema_dwh import s3io; from cinema_dwh.paths import checks_key; \
print(json.dumps(s3io.read_json(checks_key('$rid', '$name')), ensure_ascii=False, indent=2))" \
      > "$OUT/$prefix-$name.json" 2>/dev/null
    echo "  saved $prefix-$name.json"
  done
}

# Контрольная сумма бизнес-полей витрин - чтобы сравнить «до» и «после».
CHECKSUM_SQL="SELECT * FROM (
SELECT 'batch_views_minute' AS t, count() AS rows, sum(cityHash64(minute_start, content_id, views_count, watch_time_seconds_sum)) AS business_hash FROM cinema.batch_views_minute
UNION ALL SELECT 'batch_views_country_tier_daily', count(), sum(cityHash64(event_date, country, subscription_tier, views_count, watch_time_seconds_sum)) FROM cinema.batch_views_country_tier_daily
UNION ALL SELECT 'batch_top_content_daily', count(), sum(cityHash64(event_date, rank, content_id, views_count)) FROM cinema.batch_top_content_daily
UNION ALL SELECT 'batch_search_words_daily', count(), sum(cityHash64(event_date, word, word_count)) FROM cinema.batch_search_words_daily
) ORDER BY t"
META_SQL="SELECT DISTINCT run_id, interval_start, interval_end, processed_at FROM cinema.batch_top_content_daily WHERE event_date = '2026-01-03'"

step "Чистый стенд с набором $DATASET"
docker compose down -v
DATASET="$DATASET" docker compose up -d
until "${CH[@]}" -q "SELECT 1" >/dev/null 2>&1; do sleep 3; done
until airflow dags list 2>/dev/null | grep -q cinema_batch; do sleep 5; done

step "Сценарий 1. Обычная обработка: первая часть сообщений (available_at < $CUTOFF)"
tools python scripts/replay_kafka.py --dataset "data/$DATASET" --bootstrap-servers kafka:9092 --until-arrival "$CUTOFF" --rate 0
tools python scripts/wait_for_archive.py --dataset "data/$DATASET" --until-arrival "$CUTOFF"
sleep 5   # speed-слой тоже должен дочитать топик
save 00-speed-stage1.txt "Speed-слой после первой части: повторы учтены один раз" \
  "SELECT (SELECT count() FROM cinema.speed_views FINAL) AS unique_views, (SELECT sum(views_count) FROM cinema.speed_views_minute) AS views_in_minutes, (SELECT sum(watch_time_seconds_sum) FROM cinema.speed_views_minute) AS watch_seconds"
backfill 2026-01-01
JAN1_RUN=$(run_id_for 2026-01-01 2026-01-02)
save 01-after-jan1-report.txt "После запуска за 1 января: T = 2 января, 1 января из batch, 2–3 января из speed" clickhouse/queries/01_report_by_source.sql
save 01-after-jan1-country-tier.txt "Витрина страна × тариф после запуска за 1 января" clickhouse/queries/03_country_tier_daily.sql
save 01-after-jan1-boundary.txt "Граница версии профиля ДО позднего изменения: у 1001 весь день одна версия (RU)" \
  clickhouse/queries/08_views_enriched_boundary.sql --param_run_id="$JAN1_RUN"
save_checks "$JAN1_RUN" 01-jan1-checks
backfill 2026-01-02
backfill 2026-01-03
save 02-after-jan3-report.txt "После запусков за 2 и 3 января: T = 4 января, всё из batch" clickhouse/queries/01_report_by_source.sql
save 02-after-jan3-top5.txt "Дневной топ-5" clickhouse/queries/04_top5_daily.sql
save 02-after-jan3-words.txt "Частоты поисковых слов" clickhouse/queries/05_search_words_daily.sql
save 02-after-jan3-runs.txt "Журнал запусков" clickhouse/queries/06_runs.sql

step "Сценарий 2. Повторный запуск того же DAG run за 3 января (тот же срез)"
save 03-rerun-before.txt "Контрольные суммы бизнес-полей ДО повторного запуска" "$CHECKSUM_SQL"
save 03-rerun-meta-before.txt "Метаполя 3 января ДО повтора" "$META_SQL"
backfill 2026-01-03 --reset-dagruns -y
save 03-rerun-after.txt "Контрольные суммы бизнес-полей ПОСЛЕ повторного запуска (должны совпасть)" "$CHECKSUM_SQL"
save 03-rerun-meta-after.txt "Метаполя 3 января ПОСЛЕ повтора: тот же run_id, новое processed_at" "$META_SQL"

step "Сценарий 3. Поздние данные: вторая часть сообщений (available_at >= $CUTOFF)"
tools python scripts/replay_kafka.py --dataset "data/$DATASET" --bootstrap-servers kafka:9092 --from-arrival "$CUTOFF" --rate 0
tools python scripts/wait_for_archive.py --dataset "data/$DATASET"
sleep 5
save_cmd 04-kafka-groups.txt "Две независимые consumer group: ClickHouse и архиватор прочитали весь поток (LAG = 0)" \
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
save 04-archive-files.txt "Архив S3: файлы raw/ и число строк в каждом (повторы и поздние сообщения тоже здесь)" \
  "SELECT _path AS file, count() AS lines FROM s3(cinema_s3, filename = 'raw/**/*.jsonl', format = 'JSONAsString') GROUP BY file ORDER BY file"
save 04-speed-duplicates.txt "Повторная доставка: копии в архиве против одного просмотра в speed-слое" clickhouse/queries/09_raw_duplicates.sql
backfill 2026-01-04
JAN4_RUN=$(run_id_for 2026-01-04 2026-01-05)
save 04-late-runs.txt "Журнал: запуск за 4 января сам нашёл и пересчитал затронутые сутки" clickhouse/queries/06_runs.sql
save 04-late-country-tier.txt "Страна × тариф после пересчёта (поздний профиль учтён на дату просмотра)" clickhouse/queries/03_country_tier_daily.sql
save 04-late-boundary.txt "Граница версии профиля ПОСЛЕ позднего изменения: 11:59:59 - RU, с 12:00:00 - KZ" \
  clickhouse/queries/08_views_enriched_boundary.sql --param_run_id="$JAN4_RUN"
save 04-late-top5.txt "Топ-5 после пересчёта (позднее событие учтено)" clickhouse/queries/04_top5_daily.sql
save 04-late-report.txt "Общий отчёт после пересчёта" clickhouse/queries/01_report_by_source.sql
save 04-late-scd2.txt "SCD2-история профилей из S3 (запуск $JAN4_RUN)" clickhouse/queries/07_scd2_user.sql --param_run_id="$JAN4_RUN"
save_checks "$JAN4_RUN" 04-jan4-checks

step "Сценарий 4. Пересчёт исторического периода по параметрам запуска: 1–3 января одним запуском"
save 05-param-before.txt "Контрольные суммы ДО пересчёта по параметрам" "$CHECKSUM_SQL"
backfill 2026-01-10 -c '{"interval_start": "2026-01-01T00:00:00Z", "interval_end": "2026-01-04T00:00:00Z"}'
save 05-param-runs.txt "Журнал: запуск с интервалом из параметров [1 января, 4 января)" clickhouse/queries/06_runs.sql
save 05-param-after.txt "Контрольные суммы ПОСЛЕ (те же цифры: вход не менялся)" "$CHECKSUM_SQL"

step "Сценарий 5. Перезапуск старого запуска за 3 января после пересчёта - должен быть отклонён"
backfill 2026-01-03 --reset-dagruns -y || true
save 06-stale-rerun-runs.txt "Журнал: старый срез отклонён (status = failed), published_at сохранён - его данные по-прежнему в отчёте" clickhouse/queries/06_runs.sql
save 06-stale-rerun-checksum.txt "Контрольные суммы не изменились после отклонённого запуска" "$CHECKSUM_SQL"

step "Сценарий 6. Проваленная проверка качества останавливает публикацию"
tools python -c "from cinema_dwh import s3io; from cinema_dwh.config import load_settings; \
s3io.client().put_object(Bucket=load_settings().s3_bucket, Key='raw/events/ingest_date=2026-01-05/part-p9-o0-0.jsonl', \
Body=b'{\"event_id\":\"bad-view-1\",\"event_time\":\"2026-01-02T10:00:00Z\",\"user_id\":1001,\"event_type\":\"view\",\"content_id\":999999,\"watch_time_seconds\":30,\"query\":null,\"available_at\":\"2026-01-05T00:00:00Z\"}\n')"
echo "  в архив подложен просмотр с content_id = 999999, которого нет в каталоге"
backfill 2026-01-05 || true
JAN5_RUN=$(run_id_for 2026-01-05 2026-01-06)
save_cmd 07-quality-fail-task-states.txt "Состояния задач: quality_checks упала, publish_to_clickhouse не выполнялась (upstream_failed)" \
  docker compose exec -T airflow-scheduler airflow tasks states-for-dag-run cinema_batch "backfill__2026-01-05T00:00:00+00:00"
save_checks "$JAN5_RUN" 07-quality-fail-checks
save 07-quality-fail-checksum.txt "Контрольные суммы не изменились: публикации не было" "$CHECKSUM_SQL"
save 07-quality-fail-report.txt "Граница T не сдвинулась" clickhouse/queries/01_report_by_source.sql

save_cmd 08-dag-runs.txt "Все запуски DAG" docker compose exec -T airflow-scheduler airflow dags list-runs -d cinema_batch

step "Готово: $OUT"
ls -1 "$OUT"
