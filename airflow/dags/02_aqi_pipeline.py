from airflow.sdk import dag
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# ---- Config koneksi ----
SNOWFLAKE_CONN_ID = "snowflake_conn"
DATABASE = "WEATHER"
SILVER_SCHEMA = "SILVER"
GOLD_SCHEMA = "GOLD"

# ---- Config dbt ----
DBT_PROJECT_PATH = "/opt/airflow/dbt/skywatch"

silver_config = ProfileConfig(
    profile_name="skywatch",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "database": DATABASE,
            "schema": SILVER_SCHEMA,
        },
    ),
)

gold_config = ProfileConfig(
    profile_name="skywatch",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONN_ID,
        profile_args={
            "database": DATABASE,
            "schema": GOLD_SCHEMA,
        },
    ),
)

# dbt dijalankan LANGSUNG di environment utama Airflow (bukan virtualenv
# terpisah), karena dbt-snowflake sudah ter-install permanen di sana
# lewat _PIP_ADDITIONAL_REQUIREMENTS (dibutuhkan juga untuk proses
# parsing DAG / `dbt ls`). Menghindari overhead bikin venv baru tiap run.
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)


@dag(
    dag_id="02_aqi_pipeline",
    schedule="*/30 * * * *",  # sengaja cron, jalan tiap 30 menit
    catchup=False,
    max_active_runs=1,
    tags=["aqi", "dbt", "snowflake"],
)
def aqi_pipeline():
    silver_layer = DbtTaskGroup(
        group_id="load_aqi_silver",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=silver_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/silver,tag:aqi"]),
    )

    gold_layer = DbtTaskGroup(
        group_id="load_aqi_gold",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=gold_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/gold,tag:aqi"]),
    )

    silver_layer >> gold_layer


dag_obj = aqi_pipeline()
