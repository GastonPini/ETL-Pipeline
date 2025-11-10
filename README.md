# Aladia - Real-Time CDC ETL (Demo)

This repository contains a simple demo pipeline for Change Data Capture (CDC) → Kafka → PySpark Structured Streaming → Parquet (demo warehouse via DuckDB).

## Overview
- **CDC**: MongoDB Change Streams read by a Python script (`cdc/cdc_to_kafka.py`).
- **Queue**: Kafka topic (`cdc.users`) receives CDC events.
- **Processing**: PySpark Structured Streaming job (`spark/process_users_stream.py`) consumes Kafka, transforms events, writes Parquet.
- **Warehouse (demo)**: Parquet files read by `demo/demo_query.py` using DuckDB.

## Prerequisites
- Docker & Docker Compose
- Python 3.9+
- Java + Spark (for `spark-submit`) — optional if you use pip PySpark (better to have Spark locally for kafka connector)
- (Optional) MongoDB Compass for inspection

## Quick start (local / development)

1. Clone and enter repo:
```bash
git clone <https://github.com/GastonPini/ETL-Pipeline>
cd aladia-etl-pipeline