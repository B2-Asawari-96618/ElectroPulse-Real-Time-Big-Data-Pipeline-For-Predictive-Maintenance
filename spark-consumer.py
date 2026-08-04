from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_SPARK_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
PROCESSED_TELEMETRY_PATH = "data/silver/processed_telemetry"
PROCESSED_TELEMETRY_CHECKPOINT = "checkpoints/silver/processed_telemetry"

# ---------------------------------------------------
# Create Spark Session
# ---------------------------------------------------

spark = SparkSession.builder \
    .appName("TurbofanTelemetryStreaming") \
    .config("spark.jars.packages", KAFKA_SPARK_PACKAGE) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------
# Define Schema for Kafka JSON Messages
# ---------------------------------------------------

sensor_schema = StructType([
    StructField("sensor_1", DoubleType()),
    StructField("sensor_2", DoubleType()),
    StructField("sensor_3", DoubleType()),
    StructField("sensor_4", DoubleType()),
    StructField("sensor_5", DoubleType()),
    StructField("sensor_6", DoubleType()),
    StructField("sensor_7", DoubleType()),
    StructField("sensor_8", DoubleType()),
    StructField("sensor_9", DoubleType()),
    StructField("sensor_10", DoubleType()),
    StructField("sensor_11", DoubleType()),
    StructField("sensor_12", DoubleType()),
    StructField("sensor_13", DoubleType()),
    StructField("sensor_14", DoubleType()),
    StructField("sensor_15", DoubleType()),
    StructField("sensor_16", DoubleType()),
    StructField("sensor_17", DoubleType()),
    StructField("sensor_18", DoubleType()),
    StructField("sensor_19", DoubleType()),
    StructField("sensor_20", DoubleType()),
    StructField("sensor_21", DoubleType())
])

schema = StructType([
    StructField("engine_id", IntegerType()),
    StructField("cycle", IntegerType()),
    StructField("op_setting_1", DoubleType()),
    StructField("op_setting_2", DoubleType()),
    StructField("op_setting_3", DoubleType()),
    StructField("sensors", sensor_schema)
])

# ---------------------------------------------------
# Read Stream From Kafka
# ---------------------------------------------------

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "turbofan-telemetry") \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------------------------------
# Convert Kafka Value to String
# ---------------------------------------------------

json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# ---------------------------------------------------
# Parse JSON Data
# ---------------------------------------------------

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
)

# Flatten DataFrame
telemetry_df = parsed_df.select("data.*")

# ---------------------------------------------------
# Extract Important Sensors
# ---------------------------------------------------

processed_df = telemetry_df.select(
    col("engine_id"),
    col("cycle"),
    col("op_setting_1"),
    col("op_setting_2"),
    col("op_setting_3"),

    col("sensors.sensor_2").alias("temperature_sensor"),
    col("sensors.sensor_7").alias("pressure_sensor"),
    col("sensors.sensor_11").alias("rpm_sensor"),
    col("sensors.sensor_12").alias("fuel_flow_sensor")
)

# ---------------------------------------------------
# Basic Anomaly Detection
# ---------------------------------------------------

anomaly_df = processed_df.withColumn(
    "anomaly_status",
    when(col("temperature_sensor") > 643.0, "HIGH_TEMPERATURE")
    .when(col("pressure_sensor") < 553.0, "LOW_PRESSURE")
    .when(col("fuel_flow_sensor") > 522.0, "HIGH_FUEL_FLOW")
    .otherwise("NORMAL")
).withColumn(
    "processed_at",
    current_timestamp()
)

# ---------------------------------------------------
# Write Processed Stream to Parquet Storage
# ---------------------------------------------------

storage_query = anomaly_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", PROCESSED_TELEMETRY_PATH) \
    .option("checkpointLocation", PROCESSED_TELEMETRY_CHECKPOINT) \
    .trigger(processingTime="5 seconds") \
    .start()

# ---------------------------------------------------
# Output Stream to Console
# ---------------------------------------------------

console_query = anomaly_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

spark.streams.awaitAnyTermination()
