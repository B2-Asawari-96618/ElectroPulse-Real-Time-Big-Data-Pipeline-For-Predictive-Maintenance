# Title:- 
ElectroPulse-Real-Time-Big-Data-Pipeline-For-Predictive-Maintenance

# Project Overview:-
ElectroPulse is a real-time Big Data pipeline for monitoring turbofan engine health using the NASA CMAPSS dataset. The system ingests telemetry data through Apache Kafka, processes streaming data using Apache Spark Structured Streaming, orchestrates workflows with Apache Airflow, stores processed data in Parquet format, and provides interactive visualizations through a dashboard. The project demonstrates an end-to-end data engineering pipeline for predictive maintenance and engine health monitoring.

# Features:- 
- Real-time data ingestion using Apache Kafka
- Stream processing with Apache Spark Structured Streaming
- Batch analytics for historical engine data
- Workflow orchestration using Apache Airflow
- Data storage using Parquet format
- Interactive dashboard for engine health monitoring
- End-to-end ETL pipeline

# Architecture
                      
                           +----------------------+
                           |  NASA CMAPSS Dataset |
                           | (Train/Test/RUL TXT) |
                           +----------+-----------+
                                      |
                                      |
                                      v
                           +----------------------+
                           |   Kafka Producer     |
                           | kafka_producer.py    |
                           +----------+-----------+
                                      |
                                      |
                                      v
                           +----------------------+
                           |    Apache Kafka      |
                           |   Telemetry Topic    |
                           +----------+-----------+
                                      |
                                      |
                                      v
                      +----------------------------------+
                      | Apache Spark Structured Streaming|
                      |      spark_consumer.py           |
                      +----------+-----------------------+
                                 |
                 +---------------+---------------+
                 |                               |
                 |                               |
                 v                               v
      +----------------------+        +----------------------+
      | Silver Data Layer    |        | Stream Checkpoints   |
      | Processed Telemetry  |        | (Recovery State)     |
      | Parquet Files        |        | (Ignored in GitHub)  |
      +----------+-----------+        +----------------------+
                 |
                 |
                 v
      +------------------------------+
      | Batch Analytics              |
      | batch_analytics.py           |
      +--------------+---------------+
                     |
                     |
                     v
      +------------------------------+
      | Gold Data Layer              |
      | Engine Health Summary        |
      | Aggregated Parquet Data      |
      +--------------+---------------+
                     |
                     |
                     v
      +------------------------------+
      | Apache Airflow               |
      | engine_health_pipeline_dag.py|
      | Workflow Scheduling          |
      +--------------+---------------+
                     |
                     |
                     v
      +------------------------------+
      | Dashboard                    |
      | dashboard.py                 |
      | Visual Analytics & Monitoring|
      +------------------------------+

# Technology Stack:-
| Category      | Technologies                             |
| ------------- | ---------------------------------------- |
| Language      | Python                                   |
| Streaming     | Apache Kafka                             |
| Processing    | Apache Spark                             |
| Orchestration | Apache Airflow                           |
| Storage       | Parquet                                  |
| Visualization | Streamlit |
| Dataset       | NASA CMAPSS                              |

# Workflow:-
NASA CMAPSS Dataset
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Spark Consumer
        ↓
Silver Layer
        ↓
Batch Analytics
        ↓
Gold Layer
        ↓
Dashboard


