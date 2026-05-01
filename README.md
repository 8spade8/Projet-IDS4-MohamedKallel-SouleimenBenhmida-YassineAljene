# Energy Fraud Detection — Smart Meter Pipeline

## Overview

This project is a Docker Compose stack for a fraud detection pipeline using:
- Apache Hadoop HDFS
- Apache Hive Metastore and HiveServer2
- Apache Kafka
- Apache Spark Structured Streaming
- A Python smart meter simulator

The current working setup is based on `docker-compose.yml` and local service containers.

## Current Project Structure

```
projet kafka/
├── docker-compose.yml
├── README.md
├── hadoop-config/
│   ├── core-site.xml
│   └── hdfs-site.xml
├── hive/
│   ├── Dockerfile
│   ├── create_tables.hql
│   └── fraud_queries.hql
├── kafka/
│   └── kafka_consumer.py
├── simulator/
│   ├── Dockerfile
│   └── smart_meter_simulator.py
├── spark/
│   └── spark_streaming_job.py
└── report/
    └── generate_report.py
```

## What was changed

1. `docker-compose.yml`
   - Fixed `spark-job` startup so the full shell command runs with `bash -lc`.
   - Fixed `kafka-init` and `hdfs-init` init services so they run once and exit successfully.
   - Switched `spark-master` and `spark-worker` to explicit Apache Spark startup commands.
   - Added healthchecks and proper dependency sequencing.

2. `hadoop-config/hdfs-site.xml`
   - Corrected Hadoop XML property names.
   - Added `dfs.namenode.edits.dir`.
   - Set `dfs.permissions=false` for the local test environment.

3. `hive/Dockerfile`
   - Added the PostgreSQL JDBC driver required by Hive Metastore.

4. `spark-job` service
   - Uses `/opt/spark/bin/spark-submit` with `spark-sql-kafka-0-10_2.12:3.4.0` package.
   - Uses HDFS configuration via mounted `core-site.xml`.

## How to run

From the repository root:

```bash
cd "c:/Users/USER/Documents/study doc/ids4_s2/cloud big data/projet kafka"
docker compose up --build -d
```

Check the running containers:

```bash
docker compose ps --all
```

Follow logs for key services:

```bash
docker logs -f spark-job

docker logs -f simulator

docker logs -f hive-metastore
```

## Key service endpoints

| Service | Local URL / Port |
|---------|------------------|
| HDFS NameNode UI | http://localhost:9870 |
| Live Streamlit Dashboard | http://localhost:8501 |
| HDFS RPC | hdfs://namenode:9000 |
| Kafka broker | localhost:9092 |
| Hive Metastore | localhost:9083 |
| HiveServer2 | localhost:10000 |
| Spark Master UI | http://localhost:18080 |
| Spark Master RPC | spark://spark-master:7077 |

## Result verification

- Simulator is producing meter events to Kafka.
- `kafka-init` creates the topic `energy_readings`.
- `hdfs-init` creates HDFS directories under:
  - `/data/energy/raw`
  - `/data/energy/processed`
  - `/data/energy/alerts`
  - `/user/hive/warehouse`
- Spark job reads from Kafka and writes processed output to HDFS.
- Hive Metastore starts successfully with PostgreSQL and is available on port `9083`.

## Inspect results

- Use HDFS NameNode UI: `http://localhost:9870`
- Use live fraud dashboard: `http://localhost:8501`
- Use Spark UI: `http://localhost:18080`
- Query Hive via Beeline or JDBC on `localhost:10000`
- Tail simulator logs:
  ```bash
docker logs -f simulator
```
- Tail Spark job logs:
  ```bash
docker logs -f spark-job
```

## Notes

- The current stack is containerized; manual Hadoop/Kafka startup is not required.
- The stack includes a working Hive custom image with PostgreSQL JDBC driver.
- If a service stops, inspect logs and restart with:
  ```bash
docker compose restart <service-name>
```
