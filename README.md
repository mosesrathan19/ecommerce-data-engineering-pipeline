# E-commerce Data Engineering ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
![ETL](https://img.shields.io/badge/Architecture-Bronze%20%7C%20Silver%20%7C%20Gold-green)

A portfolio-ready, reproducible **100K-order** batch ETL project designed to run in **Google Colab** or locally. All data is **synthetic** — there is no real customer information.

## Architecture

**Phase 1 (original):** `Raw CSV → Pandas Silver → SQLite Gold`  
**Phase 2:** `Raw CSV (Bronze) → PySpark validate/enrich → Silver Parquet + quarantine Parquet → Gold Parquet → Spark SQL`  
**Phase 3:** `Silver snapshot + simulated CDC events → last-event-wins → validate I/U → keyed upsert/delete → immutable Parquet snapshot → Gold reporting`  
**Phase 4:** `Airflow 3 DAG → PySpark full load → CDC demo → reconciliation + monitoring`  
**Phase 5:** `AWS S3 / Unity Catalog Volume → Databricks Delta Bronze/Silver → CDC MERGE → Gold reporting`

`CSV sources (Bronze) → Python/Pandas validation and enrichment (Silver) → SQLite + monthly/category/state aggregates (Gold) → SQL & visualization`

## All 5 phases at a glance

| Phase | What it teaches | Start here |
|---|---|---|
| **1 — ETL** | Pandas, SQL, quality checks, SQLite | [Phase 1 Colab](notebooks/01_ecommerce_etl_colab.ipynb) |
| **2 — Spark** | PySpark, Parquet, Bronze/Silver/Gold | [Phase 2 Colab](notebooks/02_pyspark_parquet_colab.ipynb) |
| **3 — Incremental** | CDC simulation, upsert/delete, idempotent replay | [Phase 3 Colab](notebooks/03_incremental_cdc_colab.ipynb) |
| **4 — Airflow** | Task dependencies, retries, logs, DAG triggers | [Docker setup](docs/PHASE4_AIRFLOW.md) |
| **5 — Cloud** | S3 source, Databricks, Unity Catalog, Delta MERGE | [Databricks setup](cloud/databricks/README.md) |

**Use Phases 1–3 in Colab.** Phase 4 uses local Docker, because Colab is not a persistent Airflow server. Phase 5 needs a Databricks workspace; optional S3 storage needs an AWS account with configured access and may incur charges.

**Direct launches:** [Phase 1 Colab](https://colab.research.google.com/github/mosesrathan19/ecommerce-data-engineering-pipeline/blob/main/notebooks/01_ecommerce_etl_colab.ipynb) · [Phase 2 Colab](https://colab.research.google.com/github/mosesrathan19/ecommerce-data-engineering-pipeline/blob/main/notebooks/02_pyspark_parquet_colab.ipynb) · [Phase 3 Colab](https://colab.research.google.com/github/mosesrathan19/ecommerce-data-engineering-pipeline/blob/main/notebooks/03_incremental_cdc_colab.ipynb).

See the [full roadmap](docs/ROADMAP.md) for environment requirements and limitations.

## Dataset
- `data/raw/orders_part_01.csv` through `orders_part_20.csv`: **100,100 rows** across 20 CSV shards, including 100 duplicate order IDs; 100,000 distinct orders
- `data/raw/customers.csv`: 5,000 synthetic customers
- `data/raw/products.csv`: 200 synthetic products
- Deliberate invalid rows: 625 orders referencing an unknown customer ID

## Key Data Engineering Techniques
- Deterministic synthetic sample datasets (pre-generated, committed CSV shards)
- Source extraction, schema conversion, duplicate removal
- Referential integrity checks between fact and dimension tables
- Quarantine of invalid rows; audited record counts
- Dimensional enrichment via validated joins
- Repeatable batch pipeline, idempotent **full-refresh** loads (replace mode)
- Silver fact table and Gold monthly, category, and state aggregates
- SQL analytics in SQLite, matplotlib reporting, unit tests

## Phase 3 — Incremental CDC, Upserts, Deletes and Idempotency

**[Open Phase 3 in Google Colab](https://colab.research.google.com/github/mosesrathan19/ecommerce-data-engineering-pipeline/blob/main/notebooks/03_incremental_cdc_colab.ipynb)**

This step introduces simulated Change Data Capture (CDC) concepts. The job keeps the existing Phase 2 Silver dataset as a baseline, applies *only* incoming changed order IDs, and writes an updated Parquet snapshot to a **new version directory**. It does not overwrite the read-side Parquet files.

**One demo batch**:
| Measure | Rows |
|---|---:|
| Starting Silver snapshot | 99,375 |
| Raw CDC events | 560 |
| Insert events | 500 |
| Update events | 40 |
| Delete events | 10 |
| Stale duplicate update events | 10 |
| Effective latest events (after deduplication) | 550 |
| Final current-state snapshot | 99,865 |

We use event timestamps to pick the newest event per order in this batch; same-key/same-timestamp ambiguity is rejected. For idempotency, the notebook applies the same batch twice to a Spark DataFrame and asserts full row equality with `exceptAll`. A completed-batch manifest allows the `run_demo` entry point to return the saved output without reprocessing the same batch ID.

Run cells from top to bottom in **[notebooks/03_incremental_cdc_colab.ipynb](notebooks/03_incremental_cdc_colab.ipynb)**. Run the integration tests with `pytest -q tests/test_incremental_pipeline.py` after installing PySpark.

**Output paths:**
- `outputs/incremental/baseline/orders/` — original Silver snapshot
- `outputs/incremental/snapshots/2026-11-20-demo/orders/` — merged Silver snapshot
- `outputs/incremental/snapshots/2026-11-20-demo/gold/` — Gold aggregates
- `outputs/incremental/snapshots/2026-11-20-demo/manifest.json` — batch completion report

**Scope and limitations:** Events are simulated, not captured from a database log. This demo writes whole new Parquet snapshots, not row-level transactional MERGE operations. Gold aggregates are recalculated. It does not maintain persisted per-key versions across *different* batches or support concurrency-safe commits, recovery from partial writes, or real streaming offsets. Use transactional table formats (Delta Lake / Iceberg), checkpoints, and durable object storage for production systems. Colab output is temporary.

## Phase 2 — PySpark + Parquet (Google Colab)

**[Open Phase 2 notebook in Google Colab](https://colab.research.google.com/github/mosesrathan19/ecommerce-data-engineering-pipeline/blob/main/notebooks/02_pyspark_parquet_colab.ipynb)**

1. Open the link and select **Runtime → Run all** (or run one cell at a time to learn).
2. The notebook installs PySpark, clones this GitHub repository, and starts a local SparkSession.
3. Inspect Bronze, Silver, quarantine, Gold Parquet, Spark SQL results, and revenue charts.
4. Phase 2 uses the same source dataset and checks the same **100,100 → 100,000 → 99,375 accepted + 625 quarantined** counts.

Run Phase 2 locally with PySpark installed: 

```bash
pip install 'pyspark>=3.5,<4'
python -m src.spark_pipeline --raw-dir data/raw --output-dir outputs/spark
```

Output folders (excluded from Git) include `outputs/spark/silver/orders/`, `outputs/spark/quarantine/orders/`, `outputs/spark/gold/` and a JSON quality report. Spark writes Parquet as **directories of part-files**, not single CSV-like files. This is a full-refresh demonstration on Colab's local Spark, not a multi-node production deployment.

## Quick start in Colab
1. Open [the notebook in Colab](https://colab.research.google.com/github/mosesrathan19/ecommerce-data-engineering-pipeline/blob/main/notebooks/01_ecommerce_etl_colab.ipynb).
2. Run the cells in order. The first code cell clones this public repository automatically.
3. The notebook installs dependencies, imports the pipeline, runs ETL, shows quality metrics, SQL results, and charts.

## Run locally
```bash
pip install -r requirements.txt
python -m src.pipeline
pytest -q
```

## Expected quality checks
| Metric | Value |
|---|---:|
| Raw order rows | 100,100 |
| Duplicates removed | 100 |
| Rejected unique orders | 625 |
| Accepted clean orders | 99,375 |

Outputs are written to `outputs/` and intentionally excluded from Git. After running, inspect `quality_report.json`, `rejected_orders.csv`, the Silver table and Gold aggregates.

## SQL Example
```sql
SELECT category, ROUND(SUM(recognized_revenue),2) AS revenue
FROM silver_orders
GROUP BY category
ORDER BY revenue DESC;
```

## Portfolio talking points
- Built a repeatable batch ETL pipeline over 100K synthetic e-commerce transactions.
- Enforced primary-key, referential-integrity, schema, and numeric validation checks.
- Separated invalid records into a quarantine file and reconciled source-to-target counts.
- Published analytics-ready aggregates to SQLite for SQL reporting.

## Limitations and next steps
Phase 1 uses Pandas and SQLite; Phase 2 adds PySpark and Parquet using a local Spark runtime in Colab. Colab is not a 24/7 scheduler. Phase 3 adds simulated CDC and Parquet snapshot upserts. Phase 4 adds a local Airflow orchestration example. Phase 5 adds an AWS S3/Databricks Delta Lake deployment path. Future enhancements: durable source checkpoints, transactional production CDC with sequence handling across batches, observability, and cloud infrastructure automation.

## License
MIT. Synthetic data generated for demonstration.