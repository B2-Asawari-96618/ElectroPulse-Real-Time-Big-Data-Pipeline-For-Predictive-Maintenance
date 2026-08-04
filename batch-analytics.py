from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max,
    min,
    round,
    sum,
    when,
)

SILVER_TELEMETRY_PATH = "data/silver/processed_telemetry"
GOLD_ENGINE_HEALTH_PATH = "data/gold/engine_health_summary"


def main():
    if not Path(SILVER_TELEMETRY_PATH).exists():
        raise FileNotFoundError(
            f"Silver telemetry path not found: {SILVER_TELEMETRY_PATH}. "
            "Run spark-consumer.py first to write processed telemetry."
        )

    spark = SparkSession.builder \
        .appName("TurbofanEngineHealthAnalytics") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    telemetry_df = spark.read.parquet(SILVER_TELEMETRY_PATH)

    if telemetry_df.limit(1).count() == 0:
        spark.stop()
        raise ValueError(
            f"No records found in {SILVER_TELEMETRY_PATH}. "
            "Stream some telemetry before running analytics."
        )

    telemetry_df = telemetry_df.withColumn(
        "computed_anomaly_status",
        when(col("temperature_sensor") > 643.0, "HIGH_TEMPERATURE")
        .when(col("pressure_sensor") < 553.0, "LOW_PRESSURE")
        .when(col("fuel_flow_sensor") > 522.0, "HIGH_FUEL_FLOW")
        .otherwise("NORMAL")
    )

    health_summary_df = telemetry_df.groupBy("engine_id").agg(
        count("*").alias("total_records"),
        min("cycle").alias("first_cycle"),
        max("cycle").alias("latest_cycle"),
        round(avg("temperature_sensor"), 2).alias("avg_temperature"),
        round(max("temperature_sensor"), 2).alias("max_temperature"),
        round(avg("pressure_sensor"), 2).alias("avg_pressure"),
        round(avg("rpm_sensor"), 2).alias("avg_rpm"),
        round(avg("fuel_flow_sensor"), 2).alias("avg_fuel_flow"),
        sum(when(col("computed_anomaly_status") == "NORMAL", 1).otherwise(0)).alias("normal_count"),
        sum(when(col("computed_anomaly_status") == "HIGH_TEMPERATURE", 1).otherwise(0)).alias("high_temperature_count"),
        sum(when(col("computed_anomaly_status") == "LOW_PRESSURE", 1).otherwise(0)).alias("low_pressure_count"),
        sum(when(col("computed_anomaly_status") == "HIGH_FUEL_FLOW", 1).otherwise(0)).alias("high_fuel_flow_count"),
        max("processed_at").alias("last_processed_at"),
    )

    health_summary_df = health_summary_df.withColumn(
        "total_anomaly_count",
        col("high_temperature_count")
        + col("low_pressure_count")
        + col("high_fuel_flow_count")
    )

    health_summary_df = health_summary_df.withColumn(
        "health_status",
        when(col("total_anomaly_count") > 20, "CRITICAL")
        .when(col("total_anomaly_count") > 5, "WARNING")
        .when(col("max_temperature") > 643.5, "WARNING")
        .otherwise("HEALTHY")
    ).withColumn(
        "analytics_updated_at",
        current_timestamp()
    )

    health_summary_df.write \
        .mode("overwrite") \
        .parquet(GOLD_ENGINE_HEALTH_PATH)

    print(f"Gold engine health summary written to: {GOLD_ENGINE_HEALTH_PATH}")
    health_summary_df.orderBy("engine_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
