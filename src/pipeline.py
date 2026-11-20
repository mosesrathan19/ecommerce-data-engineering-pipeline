"""Reproducible batch ETL with raw (bronze), validated (silver), and reporting (gold) layers."""
from pathlib import Path
import argparse
import logging
import sqlite3
import pandas as pd

LOG = logging.getLogger("ecommerce_etl")
EXPECTED = {"completed", "returned", "cancelled"}

def extract(raw_dir: Path):
    orders = pd.read_csv(raw_dir / "orders.csv", dtype={"order_id": "string", "customer_id": "string", "product_id": "string"})
    customers = pd.read_csv(raw_dir / "customers.csv", dtype={"customer_id": "string"})
    products = pd.read_csv(raw_dir / "products.csv", dtype={"product_id": "string"})
    return orders, customers, products

def transform(orders, customers, products):
    before = len(orders)
    orders = orders.drop_duplicates(subset="order_id", keep="first").copy()
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    for col in ("quantity", "unit_price", "discount_pct"):
        orders[col] = pd.to_numeric(orders[col], errors="coerce")
    known_customers = set(customers.customer_id.dropna())
    known_products = set(products.product_id.dropna())
    valid = (orders.order_id.notna() & orders.customer_id.isin(known_customers)
             & orders.product_id.isin(known_products) & orders.order_date.notna()
             & orders.quantity.gt(0) & orders.unit_price.ge(0)
             & orders.discount_pct.between(0, 1) & orders.status.isin(EXPECTED))
    rejected = orders.loc[~valid].copy()
    clean = orders.loc[valid].copy()
    clean = clean.merge(customers[["customer_id", "state", "acquisition_channel"]], on="customer_id", how="left", validate="many_to_one")
    clean = clean.merge(products[["product_id", "product_name", "category"]], on="product_id", how="left", validate="many_to_one")
    clean["gross_amount"] = (clean.quantity * clean.unit_price).round(2)
    clean["net_amount"] = (clean.gross_amount * (1-clean.discount_pct)).round(2)
    # Recognize revenue for completed orders only; returns and cancellations are excluded.
    clean["recognized_revenue"] = clean.net_amount.where(clean.status.eq("completed"), 0.0)
    clean["order_month"] = clean.order_date.dt.strftime("%Y-%m")
    quality = {"raw_rows": before, "unique_orders": len(orders),
               "duplicates_removed": before-len(orders), "rejected_rows": len(rejected),
               "accepted_rows": len(clean), "all_rows_accounted_for": len(clean)+len(rejected)==len(orders)}
    assert quality["all_rows_accounted_for"]
    assert clean.order_id.is_unique
    assert clean.recognized_revenue.ge(0).all()
    return clean, rejected, quality

def load(clean, rejected, quality, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    clean.to_csv(output_dir / "silver_orders.csv", index=False)
    rejected.to_csv(output_dir / "rejected_orders.csv", index=False)
    with (output_dir / "quality_report.json").open("w") as f:
        import json
        json.dump(quality, f, indent=2)
    monthly = clean.groupby("order_month", as_index=False).agg(orders=("order_id", "count"), revenue=("recognized_revenue", "sum"))
    categories = clean.groupby("category", as_index=False).agg(orders=("order_id", "count"), revenue=("recognized_revenue", "sum"))
    states = clean.groupby("state", as_index=False).agg(orders=("order_id", "count"), revenue=("recognized_revenue", "sum"))
    monthly.to_csv(output_dir / "gold_monthly_revenue.csv", index=False)
    categories.to_csv(output_dir / "gold_category_revenue.csv", index=False)
    states.to_csv(output_dir / "gold_state_revenue.csv", index=False)
    with sqlite3.connect(output_dir / "ecommerce.db") as conn:
        clean.to_sql("silver_orders", conn, if_exists="replace", index=False)
        monthly.to_sql("gold_monthly_revenue", conn, if_exists="replace", index=False)
        categories.to_sql("gold_category_revenue", conn, if_exists="replace", index=False)
        states.to_sql("gold_state_revenue", conn, if_exists="replace", index=False)
    return monthly, categories, states

def run(raw_dir: Path, output_dir: Path):
    raw = extract(raw_dir)
    clean, rejected, quality = transform(*raw)
    monthly, categories, states = load(clean, rejected, quality, output_dir)
    LOG.info("quality=%s", quality)
    LOG.info("recognized_revenue=%.2f", clean.recognized_revenue.sum())
    return quality, monthly, categories, states

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.raw_dir, args.output_dir)

if __name__ == "__main__":
    main()
