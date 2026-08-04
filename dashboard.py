from pathlib import Path

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).parent
SILVER_TELEMETRY_PATH = PROJECT_DIR / "data" / "silver" / "processed_telemetry"
GOLD_ENGINE_HEALTH_PATH = PROJECT_DIR / "data" / "gold" / "engine_health_summary"


st.set_page_config(
    page_title="Electropulse Telemetry Dashboard",
    page_icon="",
    layout="wide",
)


@st.cache_resource
def get_spark():
    spark = SparkSession.builder \
        .appName("ElectropulseTelemetryDashboard") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@st.cache_data(ttl=30)
def load_gold_summary():
    if not GOLD_ENGINE_HEALTH_PATH.exists():
        return pd.DataFrame()

    spark = get_spark()
    return spark.read.parquet(str(GOLD_ENGINE_HEALTH_PATH)).toPandas()


@st.cache_data(ttl=30)
def load_silver_telemetry():
    if not SILVER_TELEMETRY_PATH.exists():
        return pd.DataFrame()

    spark = get_spark()
    return spark.read.parquet(str(SILVER_TELEMETRY_PATH)).toPandas()


def health_badge_counts(gold_df):
    if gold_df.empty or "health_status" not in gold_df.columns:
        return pd.DataFrame(columns=["health_status", "engine_count"])

    return (
        gold_df.groupby("health_status", as_index=False)
        .size()
        .rename(columns={"size": "engine_count"})
        .sort_values("engine_count", ascending=False)
    )


def latest_engine_snapshot(silver_df):
    if silver_df.empty:
        return silver_df

    return (
        silver_df.sort_values(["engine_id", "cycle"])
        .groupby("engine_id", as_index=False)
        .tail(1)
        .sort_values("engine_id")
    )


def add_display_anomaly_status(silver_df):
    if silver_df.empty:
        return silver_df

    silver_df = silver_df.copy()
    silver_df["display_anomaly_status"] = "NORMAL"
    silver_df.loc[
        silver_df["temperature_sensor"] > 643.0,
        "display_anomaly_status",
    ] = "HIGH_TEMPERATURE"
    silver_df.loc[
        (silver_df["display_anomaly_status"] == "NORMAL")
        & (silver_df["pressure_sensor"] < 553.0),
        "display_anomaly_status",
    ] = "LOW_PRESSURE"
    silver_df.loc[
        (silver_df["display_anomaly_status"] == "NORMAL")
        & (silver_df["fuel_flow_sensor"] > 522.0),
        "display_anomaly_status",
    ] = "HIGH_FUEL_FLOW"
    return silver_df


gold_df = load_gold_summary()
silver_df = add_display_anomaly_status(load_silver_telemetry())

st.title("Electropulse Telemetry Dashboard")

if gold_df.empty:
    st.error("Gold engine health summary is not available yet.")
    st.code("python3 batch-analytics.py", language="bash")
    st.stop()

gold_df = gold_df.sort_values("engine_id")

total_engines = int(gold_df["engine_id"].nunique())
total_records = int(gold_df["total_records"].sum())
critical_count = int((gold_df["health_status"] == "CRITICAL").sum())
warning_count = int((gold_df["health_status"] == "WARNING").sum())
normal_count = int((gold_df["health_status"] == "NORMAL").sum())


metric_cols = st.columns(4)
metric_cols[0].metric("Engines", total_engines)
metric_cols[1].metric("Telemetry Records", f"{total_records:,}")
metric_cols[2].metric("Warnings", warning_count)
metric_cols[3].metric("Critical", critical_count)

status_counts_df = health_badge_counts(gold_df)
latest_df = latest_engine_snapshot(silver_df)

summary_tab, trend_tab, anomaly_tab, raw_tab = st.tabs(
    ["Engine Health", "Temperature Trends", "Anomalies", "Telemetry Records"]
)

with summary_tab:
    left_col, right_col = st.columns([2, 1])

    with left_col:
        st.subheader("Engine Health Summary")
        st.dataframe(
            gold_df[
                [
                    "engine_id",
                    "total_records",
                    "first_cycle",
                    "latest_cycle",
                    "avg_temperature",
                    "max_temperature",
                    "avg_pressure",
                    "avg_rpm",
                    "avg_fuel_flow",
                    "total_anomaly_count",
                    "health_status",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with right_col:
        st.subheader("Health Status")
        st.bar_chart(
            status_counts_df,
            x="health_status",
            y="engine_count",
            use_container_width=True,
        )

with trend_tab:
    if silver_df.empty:
        st.warning("Silver telemetry data is not available yet.")
    else:
        engine_options = sorted(silver_df["engine_id"].dropna().unique())
        selected_engine = st.selectbox("Engine", engine_options)

        engine_df = (
            silver_df[silver_df["engine_id"] == selected_engine]
            .sort_values("cycle")
            .copy()
        )

        st.subheader("Temperature by Cycle")
        st.line_chart(
            engine_df,
            x="cycle",
            y="temperature_sensor",
            use_container_width=True,
        )

        st.subheader("Latest Sensor Snapshot")
        st.dataframe(
            latest_df[
                [
                    "engine_id",
                    "cycle",
                    "temperature_sensor",
                    "pressure_sensor",
                    "rpm_sensor",
                    "fuel_flow_sensor",
                    "display_anomaly_status",
                    "processed_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

with anomaly_tab:
    anomaly_columns = [
        "engine_id",
        "normal_count",
        "high_temperature_count",
        "low_pressure_count",
        "high_fuel_flow_count",
        "total_anomaly_count",
        "health_status",
    ]

    st.subheader("Anomaly Counts by Engine")
    st.dataframe(
        gold_df[anomaly_columns],
        use_container_width=True,
        hide_index=True,
    )

    anomaly_chart_df = gold_df[
        [
            "engine_id",
            "high_temperature_count",
            "low_pressure_count",
            "high_fuel_flow_count",
        ]
    ].set_index("engine_id")

    st.bar_chart(anomaly_chart_df, use_container_width=True)

with raw_tab:
    st.subheader("Recent Processed Telemetry")

    if silver_df.empty:
        st.warning("Silver telemetry data is not available yet.")
    else:
        recent_df = silver_df.sort_values("processed_at", ascending=False).head(200)
        st.dataframe(
            recent_df[
                [
                    "engine_id",
                    "cycle",
                    "temperature_sensor",
                    "pressure_sensor",
                    "rpm_sensor",
                    "fuel_flow_sensor",
                    "display_anomaly_status",
                    "processed_at",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
