from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = Path("/home/sunbeam/Desktop/Electropulse")
PYTHON_BIN = "python3"

default_args = {
    "owner": "electropulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="engine_health_pipeline",
    description="Build Gold engine health summaries from Silver propulsion telemetry.",
    default_args=default_args,
    start_date=datetime(2026, 5, 21),
    schedule=timedelta(minutes=15),
    catchup=False,
    tags=["electropulse", "spark", "telemetry"],
) as dag:
    check_silver_data_exists = BashOperator(
        task_id="check_silver_data_exists",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        test -d data/silver/processed_telemetry
        find data/silver/processed_telemetry -name '*.parquet' | grep -q .
        echo "Silver telemetry data found."
        """,
    )

    run_engine_health_analytics = BashOperator(
        task_id="run_engine_health_analytics",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        {PYTHON_BIN} batch-analytics.py
        """,
    )

    verify_gold_output = BashOperator(
        task_id="verify_gold_output",
        bash_command=f"""
        set -e
        cd {PROJECT_DIR}
        {PYTHON_BIN} - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyGoldEngineHealthFromAirflow").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/gold/engine_health_summary")
row_count = df.count()

if row_count == 0:
    spark.stop()
    raise SystemExit("Gold engine health summary is empty.")

print(f"Gold engine health rows: {{row_count}}")
df.orderBy("engine_id").show(truncate=False)

spark.stop()
PY
        """,
    )

    check_silver_data_exists >> run_engine_health_analytics >> verify_gold_output
