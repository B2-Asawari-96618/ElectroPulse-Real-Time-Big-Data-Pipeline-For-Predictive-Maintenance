from kafka import KafkaProducer
import pandas as pd
import json
import os
from pathlib import Path
import time

# Kafka Configuration
TOPIC_NAME = "turbofan-telemetry"
BOOTSTRAP_SERVERS = ['localhost:9092']

PROJECT_DIR = Path(__file__).parent
DATASET_DIR = PROJECT_DIR / "CMAPSSData"

# Stream enough telemetry for a portfolio-scale demo. Override from shell if needed:
# TARGET_RECORDS=100000 STREAM_DELAY_SECONDS=0.001 python3 kafka-producer.py
TARGET_RECORDS = int(os.getenv("TARGET_RECORDS", "50000"))
STREAM_DELAY_SECONDS = float(os.getenv("STREAM_DELAY_SECONDS", "0.001"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "1000"))

# Telemetry files only. RUL files and readme are intentionally excluded.
DATASET_PATHS = [
    DATASET_DIR / "test_FD001.txt",
    DATASET_DIR / "train_FD001.txt",
    DATASET_DIR / "test_FD002.txt",
    DATASET_DIR / "train_FD002.txt",
    DATASET_DIR / "test_FD003.txt",
    DATASET_DIR / "train_FD003.txt",
    DATASET_DIR / "test_FD004.txt",
    DATASET_DIR / "train_FD004.txt",
]

# Column Names
columns = [
    'engine_id',
    'cycle',
    'op_setting_1',
    'op_setting_2',
    'op_setting_3'
]

# Add sensor columns
for i in range(1, 22):
    columns.append(f'sensor_{i}')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting telemetry streaming...")
print(f"Target records: {TARGET_RECORDS:,}")
print(f"Delay per record: {STREAM_DELAY_SECONDS} seconds\n")

sent_count = 0
engine_id_offset = 0

try:
    for dataset_path in DATASET_PATHS:
        if sent_count >= TARGET_RECORDS:
            break

        print(f"Loading {dataset_path.name}...")

        df = pd.read_csv(
            dataset_path,
            sep=r"\s+",
            header=None
        )

        # Remove extra empty columns if present
        df = df.iloc[:, :26]

        # Assign column names
        df.columns = columns

        for index, row in df.iterrows():
            if sent_count >= TARGET_RECORDS:
                break

            telemetry_data = {
                "engine_id": int(row['engine_id']) + engine_id_offset,
                "cycle": int(row['cycle']),
                "op_setting_1": float(row['op_setting_1']),
                "op_setting_2": float(row['op_setting_2']),
                "op_setting_3": float(row['op_setting_3']),
                "sensors": {}
            }

            # Add sensor values
            for i in range(1, 22):
                telemetry_data["sensors"][f"sensor_{i}"] = float(row[f"sensor_{i}"])

            producer.send(TOPIC_NAME, value=telemetry_data)
            sent_count += 1

            if sent_count % FLUSH_EVERY == 0:
                producer.flush()
                print(f"Sent {sent_count:,} records...")

            if STREAM_DELAY_SECONDS > 0:
                time.sleep(STREAM_DELAY_SECONDS)

        engine_id_offset += 1000

    producer.flush()
    print(f"\nStreaming completed. Total records sent: {sent_count:,}")
finally:
    producer.close()
