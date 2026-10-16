# Banking Data Quality Pipeline

A batch data engineering pipeline that validates banking application and
credit-bureau data, separates unmatched records, and prepares datasets
for analytics.

**Tech stack:** Python · Apache Airflow · HDFS · Apache Hive · Hue ·
Docker Compose · pytest

## Getting Started

### 1. Clone the repository

``` bash
git clone https://github.com/raghulsudharsan/banking-data-quality-pipeline.git
cd banking-data-quality-pipeline
```

### 2. Set the Hue database password

``` bash
export HUE_DB_PASSWORD='your-local-development-password'
```

### 3. Start the services

``` bash
docker compose up -d
```

### 4. Start Airflow

In a separate terminal, activate your Airflow environment and start
Airflow:

``` bash
source ~/airflow_venv/bin/activate
airflow standalone
```

-   Airflow UI: http://localhost:8080
-   Hue UI: http://localhost:8888

### 5. Stop the project

Stop Airflow with `Ctrl+C` in its terminal, then run:

``` bash
docker compose down
```

## Pipeline Overview

-   Transforms application and bureau data.
-   Runs data-quality checks and separates orphan bureau records.
-   Stores processed datasets in HDFS.
-   Makes curated datasets available through Hive for SQL analysis.
-   Uses Airflow to orchestrate the pipeline.

## Run Tests

``` bash
pytest
```
