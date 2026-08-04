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


# Dataset:-

This project uses the **NASA Commercial Modular Aero-Propulsion System Simulation (CMAPSS)** dataset, a widely used benchmark for predictive maintenance and Remaining Useful Life (RUL) estimation of turbofan engines.

### Dataset Contents
- **Training Data:** `train_FD001.txt`, `train_FD002.txt`, `train_FD003.txt`, `train_FD004.txt`
- **Testing Data:** `test_FD001.txt`, `test_FD002.txt`, `test_FD003.txt`, `test_FD004.txt`
- **RUL Labels:** `RUL_FD001.txt`, `RUL_FD002.txt`, `RUL_FD003.txt`, `RUL_FD004.txt`

### Dataset Description
Each record contains:
- Engine ID
- Operational cycle
- Operational settings
- Multiple sensor measurements

The dataset simulates the degradation of turbofan engines under different operating conditions and fault modes. It is commonly used for predictive maintenance, fault diagnosis, and Remaining Useful Life (RUL) prediction.

### How It Is Used in This Project
- Telemetry data is streamed through Apache Kafka.
- Apache Spark Structured Streaming processes the incoming data.
- Processed data is stored in the Silver Layer (Parquet).
- Batch analytics generate engine health summaries in the Gold Layer.
- The dashboard visualizes key engine health metrics and analytical insights.

### Source

NASA CMAPSS (Commercial Modular Aero-Propulsion System Simulation) Turbofan Engine Degradation Simulation Dataset.


# For Installation:-
 Step 1 :- git clone <repository-url>

 Step 2 :- cd ElectroPulse-Real-Time-Big-Data-Pipeline

 Step 3 :- pip install -r requirements.txt

# To Run the project commands:-
 1) python kafka_producer.py

 2) python spark_consumer.py

 3) python batch_analytics.py

 4) python dashboard.py
 5) 
