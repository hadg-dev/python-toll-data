# Toll Data Pipeline with Airflow + Kafka

## Overview
This repository demonstrates an end-to-end data pipeline for toll-road traffic analytics using **Apache Airflow** for batch ETL and **Apache Kafka** for real-time streaming. The scenario mirrors a production-style setup where toll plazas and roadside IoT devices emit events continuously, while batch files arrive daily from upstream systems.

**What this app does:**
- **Batch ingestion (Airflow DAG)**: Downloads a daily archive of toll data, extracts structured fields from CSV/TSV/fixed-width files, consolidates them, and applies a transformation step.
- **Streaming ingestion (Kafka producer/consumer)**: Simulates real-time vehicle events, publishes them to Kafka, and loads them into a MySQL table.

The dataset and scripts are intentionally small enough to run locally, but the README details how the same pattern scales to realistic production workloads (high throughput, data retention, partitioning, and operational monitoring).

---

## Architecture
```
[Daily Files in Object Storage] ──> [Airflow DAG] ──> [Transformed Batch Output]

[IoT/Edge Events] ──> [Kafka Topic] ──> [Kafka Consumer] ──> [MySQL Table]
```

### Components
- **Airflow_DAG.py**: Batch ETL pipeline using Airflow + BashOperator.
- **data_toll_traffic_generator.py**: Kafka producer that emits simulated vehicle passages.
- **streaming_data_reader.py**: Kafka consumer that loads events into MySQL.

---

## What is Apache Airflow?
**Apache Airflow** is an open-source workflow orchestration platform. It lets you define, schedule, and monitor workflows as code. Airflow is commonly used for:
- ETL/ELT pipelines
- Scheduled data ingestion
- Dependency management between tasks
- Operational observability (logs, retries, alerts)

Airflow separates **workflow definition** (Python code) from **execution** (workers/schedulers), enabling robust production scheduling with retries, alerting, and SLA monitoring.

### What is a DAG?
A **DAG (Directed Acyclic Graph)** is a set of tasks connected by dependencies. In Airflow, a DAG defines:
- **Tasks** (work units)
- **Dependencies** (execution order)
- **Schedule** (when it runs)

The DAG in this repo runs once per day and executes these tasks in order:
1. Download and unzip the archive
2. Extract data from CSV
3. Extract data from TSV
4. Extract data from fixed-width file
5. Consolidate into a single dataset
6. Transform (upper-case normalization)

---

## What is Apache Kafka?
**Apache Kafka** is a distributed event streaming platform. It stores events in **topics**, enabling producers to publish data and consumers to process it in real time. Kafka is built for high throughput and reliability.

### Kafka Key Features
- **High throughput and low latency**: Handles millions of events per second with sub-second latency.
- **Durability**: Events are persisted to disk and replicated across brokers.
- **Scalability via partitions**: Topics are split into partitions for parallelism.
- **Consumer groups**: Multiple consumers can share work while maintaining message ordering per partition.
- **Replayability**: Consumers track offsets and can reprocess data.

---

## Production-Style Data Model
Each vehicle event represents a toll passage:
```
<timestamp>,<vehicle_id>,<vehicle_type>,<plaza_id>
```
Example:
```
Sun Jun 27 12:03:20 2026,5932012,truck,4006
```

### Realistic Production Considerations
If this were a production system, the pipeline would likely include:
- **High event volume** (hundreds of thousands per minute)
- **Multiple Kafka partitions** per topic for parallel ingestion
- **Retention policies** (e.g., 7–30 days) for replay and backfill
- **Schema validation** with tools like Confluent Schema Registry
- **Monitoring** (Airflow retries/alerts, Kafka lag, MySQL load)
- **PII handling** (hashing vehicle IDs, masking, audit logs)

---

## How the App Works
### 1) Batch ETL with Airflow
The DAG (`ETL_toll_data`) performs:
- **Download/unzip** daily files from an S3-like location
- **Extract** structured fields from CSV/TSV/fixed-width inputs
- **Consolidate** fields into a single dataset
- **Transform** values (upper-case normalization)

### 2) Streaming with Kafka
- `data_toll_traffic_generator.py` emits simulated vehicle passages.
- `streaming_data_reader.py` consumes messages from the Kafka topic and inserts them into MySQL.

---

## Running Locally
### Prerequisites
- Python 3.8+
- Kafka broker running locally (default: `localhost:9092`)
- MySQL running locally
- Airflow installed and configured

### 1) Airflow DAG
Place `Airflow_DAG.py` in your Airflow DAGs folder and trigger the DAG from the Airflow UI.

### 2) Kafka Producer
Update the topic in `data_toll_traffic_generator.py`:
```python
TOPIC = "toll"
```
Then run:
```bash
python data_toll_traffic_generator.py
```

### 3) Kafka Consumer
Update credentials in `streaming_data_reader.py` and run:
```bash
python streaming_data_reader.py
```

### Suggested MySQL Table
```sql
CREATE DATABASE tolldata;
USE tolldata;

CREATE TABLE livetolldata (
  event_timestamp DATETIME,
  vehicle_id BIGINT,
  vehicle_type VARCHAR(16),
  plaza_id INT
);
```

---

## Extending Toward Production
If you want to make this more realistic:
- Introduce **multiple partitions** and a **replication factor** in Kafka.
- Add **schema validation** and **dead-letter queues** for bad events.
- Store batch output in **object storage** with partitioned folders (date/hour).
- Add **data quality checks** in Airflow (row counts, null checks).
- Use **connection pooling** and **bulk inserts** for MySQL.

---

## License
This project is for educational purposes and demonstrations of Airflow + Kafka pipelines.
