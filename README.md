# E-commerce Data Engineering ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
![ETL](https://img.shields.io/badge/Architecture-Bronze%20%7C%20Silver%20%7C%20Gold-green)

A portfolio-ready, reproducible **100K-order** batch ETL project designed to run in **Google Colab** or locally. All data is **synthetic** — there is no real customer information.

## Architecture

**Phase 1 (original):** `Raw CSV → Pandas Silver → SQLite Gold`  
**Phase 2 (new):** `Raw CSV (Bronze) → PySpark validate/enrich → Silver Parquet + quarantine Parquet → Gold Parquet → Spark SQL`

`CSV sources (Bronze) → Python/Pandas validation and enrichment (Silver) → SQLite + monthly/category/state aggregates (Gold) → SQL & visualization`

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
Phase 1 uses Pandas and SQLite; Phase 2 adds PySpark and Parquet using a local Spark runtime in Colab. Colab is not a 24/7 scheduler. Future enhancements: Airflow, incremental loads/upserts, CI with GitHub Actions, cloud object storage.

## License
MIT. Synthetic data generated for demonstration.