# ДЗ 2.2. Оптимизация вычислений в Spark

Всё решение в одном ноутбуке: [hw2_2.ipynb](hw2_2.ipynb) (задания 1–3, планы, замеры, проверки, выводы).

## Запуск

1. Запустить Docker Desktop.
2. В папке `hw2_2`: `docker compose up -d --build`. Поднимаются S3 (RustFS), Spark (master + worker на 4 ядра и 4 ГБ) и Jupyter. Как в ДЗ 2.1, `rustfs-init` копирует входы в бакет `hw2-2`: `event_log_22/` → `events/`, `task1/campaigns.csv`, `task2/`, `task3/`.
3. Открыть Jupyter и выполнить `hw2_2.ipynb` сверху вниз (полный прогон ~20–30 минут).

Остановить: `docker compose down` (данные S3 остаются в томе; `docker compose down -v` удаляет и их).

## Что по какому адресу

| Адрес | Что открывает | Логин |
|---|---|---|
| http://localhost:8888/?token=dwh | Jupyter, здесь ноутбук | токен `dwh` уже в ссылке |
| http://localhost:4040 | Spark UI приложения из ноутбука: Jobs, Stages (shuffle, spill, время задач), SQL (физические планы). Работает, пока в ноутбуке открыта Spark-сессия | – |
| http://localhost:8080 | Spark master: воркер и запущенные приложения | – |
| http://localhost:9001 | консоль RustFS (S3): видно файлы в бакете `hw2-2` | `rustfsadmin` / `rustfsadmin` |
| http://localhost:9000 | S3 API (для программ, в браузере не нужен) | – |

## Файлы

| Файл | Зачем |
|---|---|
| `docker-compose.yml`, `spark/` | окружение (как в ДЗ 2.1) |
| `generate_event_log_1.py` | генератор событий из ДЗ 2.1; им сделаны `event_log_22/` (5 млн строк, параметры из `task1/GENERATE_EVENTS.txt`) |
| `build_scd2.py` | мой скрипт SCD2 из ДЗ 2.1 с путями ДЗ 2.2 (`s3a://hw2-2/events/` → `s3a://hw2-2/users_scd2/`); ноутбук запускает его первой ячейкой: `!python3 build_scd2.py` |
| `task1/`, `task2/`, `task3/` | данные преподавателя |

Входы в S3: `s3a://hw2-2/{events,task1,task2,task3}/`. Туда же пишутся SCD2 (`s3a://hw2-2/users_scd2/`) и результаты (`s3a://hw2-2/results/`).

Генерация событий – на хосте **до** `docker compose up` (генератору нужен Python 3.10+, в контейнере 3.8):

```
uv run python generate_event_log_1.py --rows 5000000 --users 100000 --change-probability 0.20 --seed 42 --start-time 2026-01-01T00:00:00Z --max-step-seconds 1 --user-id-start 1 --rows-per-file 100000 --output-dir event_log_22 --overwrite
```
