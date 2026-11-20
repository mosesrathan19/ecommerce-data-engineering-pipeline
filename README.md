# E-commerce Data Engineering ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
![ETL](https://img.shields.io/badge/Architecture-Bronze%20%7C%20Silver%20%7C%20Gold-green)

A portfolio-ready, reproducible **100K-order** batch ETL project designed to run in **Google Colab** or locally. All data is **synthetic** — there is no real customer information.

## Architecture
`CSV sources (Bronze) → Python/Pandas validation and enrichment (Silver) → SQLite + monthly/category/state aggregates (Gold) → SQL & visualization`

## Dataset
- `data/raw/orders.csv`: **100,100 rows**, including 100 duplicate order IDs; 100,000 distinct orders
- `data/raw/customers.csv`: 5,000 synthetic customers
- `data/raw/products.csv`: 200 synthetic products
- Deliberate invalid rows: 250 missing prices, 150 negative quantities, 125 unknown customer IDs, 100 malformed dates

## Key Data Engineering Techniques
- Deterministic synthetic sample datasets (`random.seed(42)`)
- Source extraction, schema conversion, duplicate removal
- Referential integrity checks between fact and dimension tables
- Quarantine of invalid rows; audited record counts
- Dimensional enrichment via validated joins
- Repeatable batch pipeline, idempotent **full-refresh** loads (replace mode)
- Silver fact table and Gold monthly, category, and state aggregates
- SQL analytics in SQLite, matplotlib reporting, unit tests

## Quick start in Colab
1. Upload the project ZIP or copy project files to your Google Drive; extract it.
2. Open `notebooks/01_ecommerce_etl_colab.ipynb` in [Google Colab](https://colab.research.google.com/).
3. If importing from GitHub, replace the `REPO_URL` value in the first cell with your repository URL, or upload the ZIP.
4. Run cells top to bottom. The notebook installs dependencies, imports the pipeline, runs it, shows quality metrics, SQL results, and charts.

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
This educational pipeline uses Pandas and SQLite, not distributed Spark; Colab is not a 24/7 scheduler. Future enhancements: PySpark/Parquet, Airflow, incremental loads/upserts, CI with GitHub Actions, cloud object storage.

## License
MIT. Synthetic data generated for demonstration.