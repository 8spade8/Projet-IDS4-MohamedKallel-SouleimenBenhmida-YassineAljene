# Project Brief

## What This Project Is

This repository is a containerized streaming analytics pipeline for energy fraud detection. It simulates smart-meter readings, sends them to Kafka, processes them with Spark Structured Streaming, stores raw and enriched data in HDFS, exposes that data through Hive, and generates a final analytical report from Hive query results.

In short, it is a small end-to-end data engineering project built around Kafka.

## High-Level Flow

1. `simulator/smart_meter_simulator.py` generates electricity meter readings, including realistic fraud patterns.
2. The simulator publishes JSON events to the Kafka topic `energy_readings`.
3. `spark/spark_streaming_job.py` reads the Kafka stream, adds derived metrics, applies fraud rules, and writes Parquet data to HDFS.
4. Hive external tables point at the HDFS raw, processed, and alerts datasets.
5. `report/generate_report.py` queries Hive and produces charts plus a CSV summary.

## Repository Structure

- `docker-compose.yml`: full stack orchestration for Kafka, HDFS, Hive, Spark, PostgreSQL, and the simulator.
- `hadoop-config/`: Hadoop core and HDFS configuration.
- `hive/`: Hive image build plus DDL and analysis queries.
- `kafka/`: Kafka-related helper code.
- `simulator/`: Python smart-meter event producer.
- `spark/`: Spark Structured Streaming fraud-detection job.
- `report/`: Python reporting script and report dependencies.
- `start.sh`: Bash helper for Linux/macOS.
- `start.ps1`: PowerShell helper for Windows.

## How It Works Internally

The simulator creates one reading per meter every two seconds and injects suspicious behavior for selected meters such as low reported consumption, voltage spikes, odd-hour usage, and bypass patterns.

Spark reads those events from Kafka in streaming mode, computes extra fields such as expected power and power ratio, and applies rule-based fraud detection. It then writes three outputs to HDFS:

- raw readings
- processed readings with engineered features and fraud labels
- alert-only records

Hive is then used as the SQL layer on top of those Parquet files. The report script reads aggregated results from Hive and renders a visual summary.

## How To Run It

### Prerequisites

- Docker Desktop with the Linux engine running
- Internet access for the first image build and dependency downloads
- PowerShell or a terminal opened in the repository root

### Start the full stack

```powershell
cd C:\Users\Mohamed\Desktop\projet-dali
.\start.ps1 start
```

Or with Docker Compose directly:

```powershell
docker compose up -d --build
```

### Create the Hive tables

After the containers are healthy and Spark has written some files:

```powershell
.\start.ps1 tables
```

### Check that the pipeline is working

```powershell
.\start.ps1 status
.\start.ps1 spark-logs
.\start.ps1 hdfs-ls
```

You should see:

- simulator logs continuously producing readings
- Spark batches processing Kafka events
- Parquet files under `/data/energy/raw`, `/data/energy/processed`, and `/data/energy/alerts`

## Important Endpoints

- HDFS NameNode UI: `http://localhost:9870`
- Spark Master UI: `http://localhost:8080`
- HiveServer2: `localhost:10000`
- Hive Metastore: `localhost:9083`
- Kafka for host-side clients: `localhost:29092`

Inside the Docker network, services use `kafka:9092` and `hdfs://namenode:9000`.

## Generate the Report

Create a virtual environment, install the report dependencies, then run the script:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install --upgrade pip
.\.venv\Scripts\python -m pip install -r .\report\requirements.txt
.\.venv\Scripts\python .\report\generate_report.py
```

The outputs are written to `report_output/`.

## Notes

- The stack is stateful. `docker compose down -v` deletes Kafka, HDFS, and PostgreSQL data volumes.
- The Hive DDL in `hive/create_tables.hql` has been aligned with the real Spark partition layout written to HDFS.
- If Docker image pulls fail with DNS errors, configure Docker daemon DNS servers and restart Docker Desktop.
