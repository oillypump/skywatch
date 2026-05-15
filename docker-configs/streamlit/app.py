import streamlit as st
import trino
import pandas as pd
import plotly.express as px


# ── Connection ──────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return trino.dbapi.connect(
        host="trino",
        port=8080,
        user="admin",
        catalog="iceberg",
        schema="gold",
    )


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn)


# ── Page Config ─────────────────────────────────────────────
st.set_page_config(page_title="SkyWatch", page_icon="🌤️", layout="wide")

st.title("🌤️ SkyWatch — Indonesia Air Quality & Weather")

# ── Sidebar Filter ──────────────────────────────────────────
st.sidebar.header("Filter")

cities = run_query("SELECT DISTINCT city FROM dim_city ORDER BY city")
selected_city = st.sidebar.selectbox("Pilih Kota", cities["city"].tolist())

# ── Main Data ───────────────────────────────────────────────
df = run_query(f"""
    SELECT
        f.scraped_ts,
        c.city,
        c.province,
        f.aqi,
        a.category,
        f.temp_val,
        f.humidity_val,
        f.wind_val
    FROM iceberg.gold.fact_aqi_weather f
    JOIN iceberg.gold.dim_city c ON f.city_id = c.id
    JOIN iceberg.gold.dim_aqi a ON f.aqi_id = a.id
    WHERE c.city = '{selected_city}'
    ORDER BY f.scraped_ts DESC
    LIMIT 500
""")

# ── Metrics ─────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("AQI Terkini", df["aqi"].iloc[0] if not df.empty else "-")
col2.metric("Status", df["category"].iloc[0] if not df.empty else "-")
col3.metric("Suhu (°C)", df["temp_val"].iloc[0] if not df.empty else "-")
col4.metric("Humidity (%)", df["humidity_val"].iloc[0] if not df.empty else "-")

# ── Charts ───────────────────────────────────────────────────
st.subheader("Tren AQI")
fig_aqi = px.line(df, x="scraped_ts", y="aqi", title=f"AQI - {selected_city}")
st.plotly_chart(fig_aqi, use_container_width=True)

col_l, col_r = st.columns(2)
with col_l:
    fig_temp = px.line(df, x="scraped_ts", y="temp_val", title="Suhu (°C)")
    st.plotly_chart(fig_temp, use_container_width=True)

with col_r:
    fig_hum = px.line(df, x="scraped_ts", y="humidity_val", title="Humidity (%)")
    st.plotly_chart(fig_hum, use_container_width=True)

# ── Raw Data ─────────────────────────────────────────────────
with st.expander("Raw Data"):
    st.dataframe(df, use_container_width=True)
