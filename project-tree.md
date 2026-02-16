.
├── LICENSE
├── README.md
├── airflow
│   ├── config
│   │   └── airflow.cfg
│   ├── dags
│   │   ├── 01_bronze_load_aqi_weather.py
│   │   ├── 02_dbt_load_aqi_weather.py
│   │   ├── 03_dbt_load_aqi_weather.py
│   │   ├── __pycache__
│   │   └── config.yaml
│   ├── dbt
│   │   ├── logs
│   │   ├── profiles.yml
│   │   └── skywatch_transform
│   ├── logs
│   │   ├── dag_id=01_bronze_load_aqi_weather
│   │   ├── dag_id=02_dbt_load_aqi_weather
│   │   ├── dag_id=02_silver_load_aqi_weather
│   │   ├── dag_id=03_dbt_load_aqi_weather
│   │   ├── dag_id=03_gold_load_aqi_weather
│   │   └── dag_processor
│   └── plugins
├── docker-compose.yaml
├── docker-configs
│   ├── metastore
│   │   ├── Dockerfile.metastore
│   │   └── conf
│   ├── postgres
│   │   ├── 01-init-airflow.sql
│   │   ├── 01-init-hive-metastore.sql
│   │   ├── 01-init-metabase.sql
│   │   └── 02-init-metabase_bu.sql
│   └── trino
│       └── etc
├── notes
│   ├── 02_silver_load_aqi_weather.py
│   ├── 03_gold_load_aqi_weather.py
│   ├── CASE_aqi.sql
│   ├── aqi_scraper.py
│   ├── backup_metabase.md
│   ├── gold_load_aqi_weather.py
│   ├── gold_query.sql
│   ├── query.sql
│   ├── query1.md
│   ├── re-run.md
│   ├── scraper_aqi_iceberg.py
│   ├── scraper_aqi_weather.py
│   ├── scraper_weather_iceberg.py
│   ├── silver-sql.sql
│   ├── silver_load_aqi_weather.py
│   ├── silver_load_aqi_weather_v2.py
│   ├── try-catch.md
│   └── weather_scraper.py
├── pics
│   ├── airflow_ui.png
│   ├── metabase_ui.png
│   └── trino_dbeaver.png
├── proj.md
├── project-tree.md
├── pyproject.toml
├── scraps
│   ├── config.yaml
│   ├── debug-html.py
│   ├── save-html.py
│   ├── test_1.py
│   ├── test_2.py
│   └── test_3.py
├── update-host.sh
└── uv.lock

25 directories, 46 files
