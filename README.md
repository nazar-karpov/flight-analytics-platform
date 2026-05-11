# Real-time Flight Traffic Analytics Platform

Платформа аналитики мирового авиатрафика с потоковой обработкой данных,
3-слойной архитектурой lakehouse и AI-агентом.

---

## Архитектура

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA FLOW                                 │
│                                                                  │
│  OpenSky Network API (каждые 10 сек, все борта в воздухе)       │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────┐    Kafka topic:     ┌─────────────────────────┐   │
│  │  Kafka   │    raw_flight_      │  MinIO (S3 хранилище)   │   │
│  │ Producer │    states           │                         │   │
│  └────┬─────┘         │           │  stg/flight_states/     │   │
│       │               │           │    └─ year/month/day/   │   │
│       └───────────────┤           │       (raw Parquet)     │   │
│                       │           │                         │   │
│                       ▼           │  dds/aircraft_dim/      │   │
│              ┌────────────────┐   │    └─ SCD2 dimension    │   │
│              │ Python Consumer│   │  dds/flight_states_fact/│   │
│              │ (Kafka → STG)  │──▶│    └─ fact table        │   │
│              └────────────────┘   └───────────┬─────────────┘   │
│                                               │                  │
│                                    ┌──────────▼──────────┐      │
│                                    │   Airflow (pandas)   │      │
│                                    │  ┌───────────────┐  │      │
│                                    │  │  stg_to_dds   │  │      │
│                                    │  └───────┬───────┘  │      │
│                                    │  ┌───────▼───────┐  │      │
│                                    │  │  dds_to_ddm   │  │      │
│                                    │  └───────┬───────┘  │      │
│                                    └──────────┼──────────┘      │
│                                               ▼                  │
│                                    ┌────────────────────┐       │
│                                    │    ClickHouse       │       │
│                                    │  mart_flights_curr. │       │
│                                    │  mart_airport_traf. │       │
│                                    │  mart_airline_stats │       │
│                                    │  mart_region_traff. │       │
│                                    │  mart_flight_anom.  │       │
│                                    └─────────┬──────────┘       │
│                                              │                   │
│                              ┌───────────────┴──────────┐       │
│                              ▼                           ▼       │
│                        ┌──────────┐        ┌─────────────────┐  │
│                        │ Grafana  │        │   AI Agent      │  │
│                        │Dashboard │        │ (FastAPI+чат)   │  │
│                        └──────────┘        └─────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### Слои данных (Lakehouse)

| Слой | Хранилище | Назначение |
|------|-----------|------------|
| **STG** | `s3://stg/flight_states/` | Сырые данные из Kafka, partitioned по дате/часу |
| **DDS** | `s3://dds/` | Очищенные данные; SCD2 измерение бортов + таблица фактов |
| **DDM** | ClickHouse `flights.*` | Агрегированные витрины для аналитики |

### Витрины данных (DDM)

| Витрина | Описание |
|---------|----------|
| `mart_flights_current` | Последняя позиция каждого борта в воздухе |
| `mart_airport_traffic_hourly` | Уникальные борта вблизи топ-30 аэропортов по часам |
| `mart_airline_stats_daily` | Число рейсов, средняя высота/скорость по авиакомпаниям за день |
| `mart_region_traffic_current` | Количество самолётов в воздухе по регионам |
| `mart_flight_anomalies` | Аномалии: низкая высота вдали от аэропорта, необычная скорость |

---

## Технологии

| Компонент | Технология |
|-----------|------------|
| Очередь сообщений | Apache Kafka |
| Объектное хранилище | MinIO (S3-совместимое) |
| Оркестрация | Apache Airflow |
| Обработка данных | Python + pandas |
| OLAP-хранилище | ClickHouse |
| Визуализация | Grafana |
| AI-агент | FastAPI + OpenRouter (GPT-4o-mini) |
| Контейнеризация | Docker Compose |

---

## Требования

- **Docker** 24+
- **Docker Compose** v2 (входит в Docker Desktop)
- **RAM**: 8 ГБ минимум (16 ГБ рекомендуется)
- **Диск**: ~5 ГБ для Docker-образов
- **Интернет** для OpenSky API и скачивания образов

---

## Быстрый старт

### Linux / macOS
```bash
git clone <repo-url>
cd flight-analytics-platform
cp .env.example .env
# Отредактируй .env — добавь OPENROUTER_API_KEY
docker compose up -d
```

### Windows (PowerShell)
```powershell
git clone <repo-url>
cd flight-analytics-platform
Copy-Item .env.example .env
# Отредактируй .env — добавь OPENROUTER_API_KEY
docker compose up -d
```

Первый запуск займёт **5-10 минут** (скачиваются образы).

```bash
# Проверить что всё работает
docker compose ps

# Смотреть логи продюсера
docker compose logs -f kafka-producer
```

---

## Сервисы

| Сервис | URL | Логин |
|--------|-----|-------|
| **Kafka UI** | http://localhost:8080 | — |
| **MinIO** | http://localhost:9001 | admin / password123 |
| **Airflow** | http://localhost:8082 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **AI Agent (чат)** | http://localhost:8000 | — |
| **ClickHouse** | http://localhost:8123/play | — |

---

## Пайплайн данных

### Шаг 1 — Потоковый сбор (непрерывно)
`kafka-producer` опрашивает OpenSky Network API каждые 10 секунд
и получает позиции всех самолётов в воздухе (несколько тысяч за раз).
Каждый state-vector отправляется отдельным Kafka-сообщением с ключом
`icao24`. Python-consumer читает из Kafka и пишет Parquet-файлы
в MinIO `s3://stg/flight_states/`, партиционированные по
`year/month/day/hour`.

### Шаг 2 — STG → DDS (каждые 5 минут, Airflow)
DAG `stg_to_dds`:
- Читает свежие STG-партиции
- Дедуплицирует записи по `(icao24, time_position)`
- Обогащает: `airline_code` (из callsign), `region` (из координат)
- Применяет **SCD2** для измерения `aircraft_dim`
- Дописывает записи в `flight_states_fact`

### Шаг 3 — DDS → DDM (каждые 5 минут, Airflow)
DAG `dds_to_ddm`:
- Читает DDS из MinIO через pandas
- Вычисляет 5 витрин, включая детектор аномалий
- Пишет в ClickHouse (ReplacingMergeTree)

### Начальная загрузка (опционально)
DAG `initial_load` загружает текущий снепшот OpenSky:
1. Открой http://localhost:8082
2. Найди DAG `initial_load`
3. Нажми ▶ (Trigger DAG)

---

## AI Agent — Примеры вопросов

Открой http://localhost:8000 — чат-бот для аналитики авиатрафика.

- *"Какой аэропорт сейчас самый загруженный?"*
- *"Покажи аномальные рейсы за последний час"*
- *"Сколько самолётов в воздухе над Европой прямо сейчас?"*
- *"Топ-5 авиакомпаний по числу рейсов сегодня"*
- *"Сравни среднюю скорость Аэрофлота и Emirates"*
- *"Какая средняя высота полёта у Lufthansa сегодня?"*

Агент использует tool calling — генерирует SQL-запрос к ClickHouse,
получает данные и формирует ответ на естественном языке.

---

## Мониторинг

```bash
# Статус всех сервисов
docker compose ps

# Логи конкретного сервиса
docker compose logs -f kafka-producer
docker compose logs -f spark-streaming
docker compose logs -f airflow-scheduler
docker compose logs -f ai-agent

# Запустить DAG вручную
docker compose exec airflow-scheduler airflow dags trigger stg_to_dds

# Запрос к ClickHouse
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM flights.mart_flights_current"
```

---

## Частые проблемы

| Проблема | Причина | Решение |
|----------|---------|---------|
| Grafana пустая | Витрины ещё не заполнены | Запусти `dds_to_ddm` вручную в Airflow |
| Airflow показывает ошибку ClickHouse | ClickHouse не готов | Подожди 1-2 минуты, запусти таску повторно |
| AI Agent: "Internal Server Error" | Нет API ключа OpenRouter | Добавь `OPENROUTER_API_KEY` в `.env` |
| OpenSky возвращает 429 | Слишком частые запросы без авторизации | Добавь `OPENSKY_USER`/`OPENSKY_PASS` в `.env` (регистрация бесплатная) |
| OpenSky возвращает пустой ответ | Анонимный доступ с жёсткими лимитами | Зарегистрируйся на opensky-network.org |
| Vmmem жрёт всю память (Windows) | WSL2 без лимита | Создай `~/.wslconfig` с `memory=8GB` |
| Порт занят | Другой сервис на этом порту | `docker compose down`, освободи порт |

---

## Остановка и очистка

```bash
# Остановить всё
docker compose down

# Остановить и удалить все данные
docker compose down -v
```

---

## Проектные решения

1. **pandas вместо PySpark** — упрощает архитектуру, убирает тяжёлые
   контейнера Spark, экономит ~3 ГБ RAM. Для потока ~5000 записей
   каждые 10 секунд pandas справляется.

2. **Python Kafka consumer вместо Spark Streaming** — простой скрипт
   читает из Kafka и пишет Parquet в MinIO. Надёжнее и легче дебажить.

3. **ReplacingMergeTree в ClickHouse** — позволяет идемпотентные запуски
   DAG; повторный запуск перезаписывает данные, а не дублирует.

4. **OpenSky Network free API** — бесплатный доступ к реальным данным ADS-B.
   Без авторизации ~5 запросов/10 сек, с авторизацией ~1 req/sec.
   Продюсер автоматически обрабатывает 429/503 с exponential backoff.

5. **OpenRouter + GPT-4o-mini** — надёжный tool calling для SQL-запросов.
   Можно заменить на любую модель, изменив `OPENROUTER_MODEL` в `.env`.

6. **AI-агент как чат-бот** — простой веб-интерфейс на FastAPI,
   агент сам пишет SQL-запросы через tool calling и возвращает ответ.

7. **Детектор аномалий на pandas** — простая логика в DAG `dds_to_ddm`
   (пороговые значения, не ML). Обнаруживает 2 типа аномалий:
   низкая высота вдали от аэропорта, необычная скорость (выше 95-го перцентиля).
