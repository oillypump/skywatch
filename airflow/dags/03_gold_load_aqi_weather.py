from airflow.sdk import dag, task
from trino.dbapi import connect
from datetime import datetime
from airflow.datasets import Dataset

# --- KONFIGURASI HARDCODE ---
TRINO_CONFIG = {
    "host": "trino",
    "port": 8080,
    "user": "admin",
    "catalog": "iceberg",
}

AQI_SILVER = Dataset("s3a://lakehouse/silver/aqi_index")
FORECAST_SILVER = Dataset("s3a://lakehouse/silver/weather_forecast")


@dag(
    dag_id="03_gold_load_aqi_weather",
    start_date=datetime(2026, 2, 1),
    schedule=(AQI_SILVER, FORECAST_SILVER),
    catchup=False,
    tags=["gold", "iceberg"],
)
def weather_aqi_pipeline():
    """
    LAYER : GOLD
    """

    def execute_trino(sql):
        """Helper untuk koneksi langsung ke Trino"""
        conn = connect(**TRINO_CONFIG)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            # return fetchall hanya jika ada data, kalau DDL/DML biasanya return list kosong
            try:
                return cur.fetchall()
            except:
                return None
        finally:
            conn.close()

    @task()
    def schema_preparation():
        query = """
            CREATE SCHEMA IF NOT EXISTS iceberg.gold
            WITH (location = 's3a://lakehouse/gold/')
        """
        execute_trino(query)
        print("Schema 'gold' prepared.")

    @task()
    def table_dim_city():
        queries = [
            """
            CREATE TABLE IF NOT EXISTS iceberg.gold.dim_city (
                id VARCHAR, 
                city VARCHAR, 
                province VARCHAR, 
                country VARCHAR,
                created_ts TIMESTAMP(6),
                updated_ts TIMESTAMP(6)
            ) 
            WITH (
                format = 'PARQUET', 
                location = 's3a://lakehouse/gold/dim_city/'
            )""",
            """
            MERGE INTO iceberg.gold.dim_city as target
            USING (
	            select 
                    to_hex(md5(to_utf8(city))) as id,
		            city,
		            province,
		            'Indonesia' as country
	            from (
                    select distinct city, province from iceberg.silver.aqi_index 
                ) 
            ) as source
            ON target.id = source.id

            WHEN MATCHED THEN 
                UPDATE SET 
                    updated_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
            WHEN NOT MATCHED THEN 
                INSERT (id, city, province, country, created_ts, updated_ts)
                VALUES (
                    source.id, 
                    source.city, 
                    source.province, 
                    source.country, 
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)), 
                    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6))
                )
            """,
        ]

        for q in queries:
            execute_trino(q)
        print("Table dim_city updated.")

    @task()
    def table_dim_aqi():
        queries = [
            """
            CREATE TABLE IF NOT EXISTS iceberg.gold.dim_aqi (
                id VARCHAR,
                category VARCHAR,
                min_val INTEGER,
                max_val INTEGER,
                created_ts TIMESTAMP(6),
                updated_ts TIMESTAMP(6)
            )
            WITH (
                format = 'PARQUET', 
                location = 's3a://lakehouse/gold/dim_aqi/'
            )
            """,
            """
            MERGE INTO iceberg.gold.dim_aqi as target
            using (
                select
                	to_hex(md5(to_utf8(aqi_status))) as id,
                    aqi_status as category,
                    MIN(aqi_val) as min_val,
                    MAX(aqi_val) as max_val,
            		cast(CURRENT_TIMESTAMP as timestamp) as created_ts, 
            		cast(CURRENT_TIMESTAMP as timestamp) as updated_ts 
                FROM iceberg.silver.aqi_index
                GROUP BY aqi_status
                order by min_val
            ) as source 
            on target.id = source.id 
            WHEN MATCHED THEN 
            	UPDATE set
            		min_val = source.min_val,
            		max_val = source.max_val,
            		updated_ts = source.updated_ts

            WHEN NOT MATCHED then
            	insert (
            		id,
            		category,
            		min_val,
            		max_val,
            		created_ts,
            		updated_ts
            	)
            	values (
            		source.id,
            		source.category,
            		source.min_val,
            		source.max_val,
            		source.created_ts,
            		source.updated_ts
            	)

            """,
        ]
        for q in queries:
            execute_trino(q)
        print("Table dim_aqi updated.")

    @task()
    def table_fact_aqi_weather():
        # 1. DDL
        create_query = """
            CREATE TABLE IF NOT EXISTS iceberg.gold.fact_aqi_weather (
                fact_id VARCHAR,
                city_id VARCHAR,
                event_ts TIMESTAMP(6),
                aqi INTEGER,
                aqi_id VARCHAR,
                main_pollutant VARCHAR,
                pollutant_val DOUBLE,
                weather_condition VARCHAR,
                temp_val DOUBLE,
                humidity_val INTEGER,
                wind_val DOUBLE,
                wind_dir VARCHAR,
                alert VARCHAR,
                load_ts TIMESTAMP(6),
                observation_ts TIMESTAMP(6)
            ) WITH (
                format = 'PARQUET',
                location = 's3a://lakehouse/gold/fact_aqi_weather/'
            )
        """
        execute_trino(create_query)

        # 2. Upsert menggunakan MERGE
        upsert_query = """
        MERGE INTO iceberg.gold.fact_aqi_weather AS target
        USING (
            with source as (
            	SELECT 
            		COALESCE(a.province , f.province ) as province,
            		COALESCE(a.city, f.city) as city,		
            		COALESCE(a.event_ts, f.forecast_ts) as event_ts,
                	a.aqi_val as act_aqi,
                	f.aqi_val as fc_aqi,
            		a.aqi_status as aqi_status,
            		a.main_pollutant as main_pollutant, 
            		a.pollutant_val as pollutant_val,
            		a.weather_condition as act_weather_condition,
            		f.weather_condition as fc_weather_condition,
            		a.temp_val as act_temp_val,
            		f.temp_val as fc_temp_val,
            		a.humidity_val as act_humdiy_val,
            		f.humidity_val as fc_humidity_val,
            		a.wind_val as act_wind_val,
            		f.wind_val as fc_wind_val,
            		a.wind_dir,
            		a.alert,
            		coalesce(a.scraped_ts, f.scraped_ts ) as scraped_ts,
            		f.observation_ts 
            	FROM iceberg.silver.aqi_index  a
            	FULL OUTER JOIN iceberg.silver.forecast_weather f
            	ON a.city = f.city AND a.event_ts = f.forecast_ts
            	WHERE COALESCE(a.event_ts, f.forecast_ts) >= CURRENT_DATE - INTERVAL '1' DAY
            ),
            deduplicated AS (
                    -- Di sini kita selipkan ROW_NUMBER untuk membuang duplikat
            	SELECT *,
                	ROW_NUMBER() OVER (
                    	PARTITION BY city, event_ts 
                        ORDER BY scraped_ts DESC
            		) as rn
            	FROM source
            )
            SELECT 
                to_hex(md5(to_utf8(s.city|| cast(s.event_ts as varchar)))) as fact_id,
            	s.event_ts,
                c.id as city_id,
                coalesce(s.act_aqi,s.fc_aqi) as aqi,
                a.id as aqi_id,
                coalesce(s.act_weather_condition ,s.fc_weather_condition ) as weather_condition,
                COALESCE(s.act_temp_val, s.fc_temp_val) as temp_val,
            	coalesce(s.act_humdiy_val , s.fc_humidity_val ) as humidity_val,
            	coalesce(s.act_wind_val , s.fc_wind_val ) as wind_val,
                s.pollutant_val,
                s.main_pollutant,
            	s.wind_dir,
            	s.alert ,
            	s.scraped_ts as load_ts,
            	s.observation_ts 
            FROM deduplicated s
            left join iceberg.gold.dim_city c
            	on s.city = c.city
            left join iceberg.gold.dim_aqi a
            	on coalesce(s.act_aqi,s.fc_aqi) BETWEEN a.min_val AND a.max_val
            WHERE s.rn = 1
        ) AS source
        ON target.fact_id = source.fact_id

        WHEN MATCHED THEN
            UPDATE SET
                city_id = source.city_id,
                aqi = source.aqi,
                aqi_id = source.aqi_id,
                main_pollutant = source.main_pollutant,
                pollutant_val = source.pollutant_val,
                weather_condition = source.weather_condition,
                temp_val = source.temp_val,
                humidity_val = source.humidity_val,
                wind_val = source.wind_val,
                wind_dir = source.wind_dir,
                alert = source.alert,
                load_ts = source.load_ts

        WHEN NOT MATCHED THEN
            INSERT (
                fact_id, city_id, event_ts, aqi, aqi_id, 
                main_pollutant, pollutant_val, weather_condition, 
                temp_val, humidity_val, wind_val, wind_dir, 
                alert, load_ts, observation_ts
            )
            VALUES (
                source.fact_id, source.city_id, source.event_ts, source.aqi, source.aqi_id, 
                source.main_pollutant, source.pollutant_val, source.weather_condition, 
                source.temp_val, source.humidity_val, source.wind_val, source.wind_dir, 
                source.alert, source.load_ts, source.observation_ts
            )
        """
        execute_trino(upsert_query)
        print("Fact table gold.fact_aqi_weather updated successfully.")

    # Flow DAG
    gold_preps = schema_preparation()
    gold_preps >> table_dim_city()
    gold_preps >> table_dim_aqi()
    gold_preps >> table_fact_aqi_weather()


# Inisialisasi DAG
weather_aqi_dag = weather_aqi_pipeline()
