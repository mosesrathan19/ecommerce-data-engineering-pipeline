"""Phase 2 quality and Parquet tests. Run: pytest -q tests/test_spark_pipeline.py"""
from pathlib import Path
import pytest
pytest.importorskip("pyspark")
from src.spark_pipeline import spark_session, extract, transform, gold_tables

ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture(scope="module")
def spark():
    session = spark_session()
    yield session
    session.stop()

def test_spark_counts_and_quality(spark):
    orders, customers, products = extract(spark, ROOT / "data/raw")
    unique, silver, rejected = transform(orders, customers, products)
    assert orders.count() == 100100
    assert unique.count() == 100000
    assert silver.count() == 99375
    assert rejected.count() == 625
    assert silver.select("order_id").distinct().count() == silver.count()

def test_gold_reconciles(spark):
    _, silver, _ = transform(*extract(spark, ROOT / "data/raw"))
    gold = gold_tables(silver)
    assert gold["monthly_revenue"].count() == 12
    assert gold["category_revenue"].count() == 5
    assert gold["state_revenue"].count() == 10
    assert gold["category_revenue"].selectExpr("sum(orders) AS n").first().n == 99375
