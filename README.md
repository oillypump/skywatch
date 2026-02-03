# Skywatch

SkyWatch is a robust Python-based web scraping tool designed to extract real-time air quality data and hourly forecasts from the IQAir platform. It provides structured insights into pollution levels, meteorological data, and future trends for specific regions.

## 🚀 Features

- Real-Time Data Extraction: Captures current AQI index, air quality status, and main pollutant concentration.
- Hourly Forecasting: Retrieves 24-hour forecasts including temperature, wind speed, and humidity.
- Smart Time-Tracking: Automatically handles date transitions (day-over-day) in the forecast table.
- Standardized Timestamps: Outputs data in a clean YYYY-MM-DD HH:mm:ss format, ready for database integration.
- Dynamic URL Handling: Supports region-specific scraping (e.g., Province and City levels).

## re-populate

### 1. create uv env

```bash
uv init --python=3.12.12 --name=dwh--no-readme
uv run which python
```

#### 2. create folder

```bash
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p airflow/config
```

#### 3. create .env for airflow services

copy this script run on your terminal within this folder project

```
cat <<EOF > .env
AIRFLOW_UID=$(id -u)
_AIRFLOW_WWW_USER_USERNAME=airflowuser
_AIRFLOW_WWW_USER_PASSWORD=airflowuser
AIRFLOW_PROJ_DIR=./airflow
EOF
```

#### sta

```bash
docker compose run airflow-cli airflow config list
docker compose up airflow-init
docker compose up -d
```

#### dump backup metabase

```
echo "\c metabasedb;" > ./docker-configs/postgres/02-init-metabase.sql && \
docker exec -t postgres pg_dump -U metabase metabasedb | grep -v "restrict" >> ./docker-configs/postgres/02-init-metabase.sql

```

## metabase access

```
user : admin@email.com
pass : Admin1234
```
