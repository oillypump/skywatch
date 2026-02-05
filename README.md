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

#### start all service

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

## Access

1. metabase
   ```
   url  : http://localhost:3000
   user : admin@email.com
   pass : Admin1234
   ```
2. airflow
   ```
   url  : http://localhost:8080
   user : airflowuser
   pass : airflowuser
   ```
3. trino
   ```
   url  : http://localhost:8080
   user : admin
   pass :
   ```

### READS

1. PM2.5 & PM10 (Fine and Coarse Particulate Matter)These are the "main enemies" when it comes to air quality.PM2.5 ($18.5 \mu g/m^3$): These are particles about 30 times smaller than a human hair. Because they are so tiny, they can bypass the nose and throat to enter the lungs and even the bloodstream. An index of 18.5 is considered Moderate. For context, the WHO annual guideline is below $5 \mu g/m^3$, so while it isn't "Red Alert" territory yet, it is already quite "dirty."PM10 ($19.4 \mu g/m^3$): These are slightly larger particles, like road dust or pollen. Since your PM10 reading is almost the same as your PM2.5, it means the pollution near Jakarta is currently dominated by ultra-fine combustion particles (from engines/industry) rather than just coarse soil dust.
2. $NO_2$ & $SO_2$ (Industrial & Vehicle Exhaust Gases)These two figures are the most striking in your data:$NO_2$ ($42.8 \mu g/m^3$): Nitrogen Dioxide usually comes from vehicle exhaust. This number is quite high. If you are near a main road, this gas can cause itchy eyes or a scratchy throat.$SO_2$ ($44.3 \mu g/m^3$): Sulphur Dioxide typically comes from coal burning (Power Plants) or heavy industry. A reading of 44.3 is high for a daily average. This gas is a primary cause of acid rain and can trigger asthma attacks.

3. $O_3$ (Ground-Level Ozone)$O_3$ ($2.1 \mu g/m^3$): While ozone in the upper atmosphere protects us from the sun, ozone at ground level (created by pollution reacting with sunlight) is harmful. Fortunately, your reading of 2.1 is very low. This usually happens when the weather is cloudy or after rain, as ozone needs intense sunlight to form.
