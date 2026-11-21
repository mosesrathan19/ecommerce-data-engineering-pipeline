# E-commerce Data Engineering — 5-phase learning roadmap

| Phase | Concepts | Where to run | Status |
|---|---|---|---|
| 1 | Pandas ETL, validation, SQLite, Gold reports | Google Colab/local | Code published; user ran |
| 2 | PySpark, schema, joins, Parquet, Spark SQL | Google Colab/local | Code published; user ran |
| 3 | Simulated CDC, insert/update/delete, event-time dedup, snapshot replay | Google Colab/local | Code published; Colab check needed |
| 4 | Airflow 3 DAG, dependencies, retries, logs and reconciliation | Local Docker | Code published; Docker runtime check needed |
| 5 | AWS S3 source, Unity Catalog volume, Databricks Delta tables and MERGE | Databricks workspace | Code published; cloud accounts and execution needed |

## Scope
Phases 4 and 5 are **additional deployment paths**, not a single continuously running, cloud-deployed system. Airflow is manually triggered because the source CDC feed is fixed. Phase 5 demonstrates a Databricks-managed Delta MERGE using synthetic CDC, but requires the user to configure Unity Catalog and import the notebook. Production applications need durable checkpoints, secrets, alerting, source data contracts, and safe transaction handling.
