# ДЗ 2.1. Сборка таблицы SCD2
SCD (Slowly Changing Dimension) — способ хранить справочник, который медленно меняется. 
Тип 1 просто перезаписывает значение, и история теряется. 
Тип 2 на каждое изменение добавляет новую строку с периодом, и история сохраняется.

## Файлы

| Файл | Зачем |
|---|---|
| `docker-compose.yml`, `spark/` | окружение: S3 (RustFS), Spark, Jupyter |
| `generate_event_log_1.py`, `event_log/` | скрипт преподавателя и сгенерированные данные (6459 строк, `--seed 42`) |
| `build_scd2.py` | скрипт сборки SCD2 таблицы (на ревью) |
| `hw2_1.ipynb` | запрос подсчёта строк и запрос «логин X на момент Y» с результатами |

## Запуск

1. Запустить Docker Desktop.
2. В папке проекта: `docker compose up -d --build` (поднимает S3, Spark, Jupyter и загружает `event_log/` в S3).
3. Открыть http://localhost:8888/?token=dwh, в Jupyter: File → New → Terminal, выполнить `python3 build_scd2.py`.
4. Открыть `hw2_1.ipynb` и выполнить ячейки.

Остановить: `docker compose down`.

## Результаты

- Строк в SCD2 таблице: **379** (`SELECT COUNT(*) FROM dim_user_scd2`).
- Пользователь 24, логин на момент Y: до первого события (00:03:53) — пусто; 00:03:54 и 01:24:15 — `ephillips24`; 01:24:16 и позже — `twalker24r2`.


http://localhost:9001 rustfsadmin rustfsadmin