# Phase 4 — Apache Airflow scheduling, retries, monitoring

Phase 4 orchestrates the existing **PySpark Phase 2 full load** and **Phase 3 simulated CDC** from a local Airflow 3.3.1 Docker container. It does not change the original code or require paid cloud services.

## Requirements
- Docker Desktop and Docker Compose v2, with enough memory for Java/Spark and Airflow (recommend 6–8 GB free).
- Run these commands on **your own laptop or server**, NOT Google Colab (whose sessions stop automatically).

## Start
From the repository root:

```bash
cd airflow
docker compose up --build -d
docker compose logs -f airflow
```

Once Airflow is ready, open http://localhost:8080. The login details for `airflow standalone` appear in the container output. Find DAG `ecommerce_etl_phases_2_and_3` and **Trigger DAG**.

## Pipeline tasks
1. `phase2_full_refresh` — raw CSV → Silver/Gold Parquet, checks 100100 → 99375.
2. `phase3_incremental_cdc` — simulate 560 events and write a new snapshot; 99865 final orders.
3. `verify_outputs` — ensure expected counts and manifest are present.

Airflow displays status, retries, durations, and logs for each task. The DAG uses retries and prevents overlapping runs. Run `docker compose down` to stop. `docker compose down -v` also removes the persistent Airflow metadata volume.

## Scheduling and production caveats
This DAG is **manually triggered** because the current CDC example uses a single fixed demo batch ID. Repeated automatic daily runs without new changes would be misleading: they simply verify the same sample again. To schedule actual daily changes, first accept a new immutable batch of CDC events (or Kafka/CDC offsets), generate date-based batch IDs, and persist the current committed snapshot and checkpoints. Then change `schedule=None` to e.g. `schedule="0 6 * * *"`.

This Docker container, credentials, bundled SQLite metadata database and local Parquet snapshots are development-only. A production installation needs a supported metadata DB, distributed or managed execution, secrets, durable storage and alerting. Never publish Airflow secrets.
