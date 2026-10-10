# Swiggy-Style Data Engineering Platform

An end-to-end Data Engineering pipeline designed to simulate a food delivery platform (like Swiggy or UberEats). Built entirely on a single machine using Docker, this project implements a hybrid **Lambda Architecture** (Batch + Streaming) utilizing a modern data stack.

## Architecture & Tech Stack

This project was built to run in a resource-constrained environment (8GB RAM) by heavily optimizing Docker containers and using embedded analytical databases.

* **OLTP Database:** PostgreSQL
* **Streaming / Message Broker:** Redpanda (Kafka-compatible)
* **Data Processing:** Apache Spark (PySpark) & Spark Structured Streaming
* **Storage Layer:** Delta Lake (Bronze/Silver/Gold Lakehouse)
* **Data Transformation:** dbt (data build tool)
* **Analytical / OLAP Engine:** DuckDB
* **Orchestration:** Apache Airflow
* **Visualization:** Apache Superset

## Project Structure

* `01_oltp_db/`: Scripts to seed and interact with the PostgreSQL relational database.
* `02_ingestion/`: PySpark batch ingestion scripts for reading Postgres dumps and writing to the Delta Lake Bronze layer.
* `03_streaming/`: Kafka producer and PySpark Structured Streaming consumer for handling real-time GPS and order events (with DLQ handling).
* `04_transform/`: dbt models to transform Bronze/Silver Delta tables into a Star Schema (Gold layer) inside DuckDB.
* `05_orchestration/`: Airflow DAGs that automate the end-to-end batch pipeline.
* `data/`: Local volume mounts for DuckDB data, Delta Lake files, and raw CSV dumps.

## Pipeline Overview

### 1. Batch Pipeline (Historical Data)
1. **Extraction:** Airflow triggers a Python extraction job that pulls static data (Restaurants, Historical Orders) from PostgreSQL and saves them as raw CSVs.
2. **Ingestion:** PySpark (`batch_ingestion.py`) reads the CSVs and writes them to the Delta Lake `bronze` tier.
3. **Transformation:** `dbt` runs models (`fact_orders`, `dim_restaurants`) to clean and join the data, outputting the final Gold tables into DuckDB.
4. **Orchestration:** All of the above is scheduled and automated via an Airflow DAG (`swiggy_batch_pipeline`).

### 2. Streaming Pipeline (Live Events)
1. **Producer:** `stream_producer.py` generates fake live order events every few seconds (injecting intentional errors 20% of the time) and pushes them to a Redpanda topic.
2. **Consumer:** PySpark Structured Streaming (`spark_streaming_job.py`) consumes the Kafka topic in real-time.
3. **Validation & DLQ:** Valid records are saved to a Delta table (`bronze_orders`), while corrupted records (e.g., negative amounts, missing IDs) are quarantined into a Dead-Letter Queue (`dlq`).
4. **Serving:** A Python script automatically maps these Delta tables as views into DuckDB so they can be queried in real-time.

### 3. Visualization
* **Apache Superset** connects to DuckDB to visualize both the batch Star Schema and the near-real-time streaming views.

## How to Run

1. Ensure Docker Desktop is running.
2. Run `docker compose up -d` to spin up all infrastructure (Postgres, Redpanda, Jupyter/Spark/Airflow, Superset).
3. Access Airflow at `http://localhost:8082` (admin / admin).
4. Access Superset at `http://localhost:8088` (admin / admin).
5. Access Redpanda Console at `http://localhost:8081`.

*(Note: Airflow and dbt are installed natively inside the `jupyter` container to optimize RAM usage).*
