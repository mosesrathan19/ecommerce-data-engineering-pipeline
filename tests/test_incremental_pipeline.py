"""Phase 3 integration tests. Run: pytest -q tests/test_incremental_pipeline.py

These use the deterministic synthetic dataset and local Spark; they are slower than
unit tests because Spark reads and validates ~100K orders.
"""
from pathlib import Path
import pytest
pytest.importorskip("pyspark")
from pyspark.sql import functions as F
from src.spark_pipeline import spark_session, extract, transform
from src.incremental_pipeline import (
    generate_cdc_events, latest_events, apply_cdc, same_rows
)

ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture(scope="module")
def data():
    spark = spark_session()
    _, customers, products = extract(spark, ROOT / "data/raw")
    raw_orders, _, _ = extract(spark, ROOT / "data/raw")
    _, baseline, _ = transform(raw_orders, customers, products)
    yield spark, baseline, customers, products
    spark.stop()

def test_counts(data):
    spark, baseline, customers, products = data
    events = generate_cdc_events(spark, baseline)
    chosen = latest_events(events)
    assert events.count() == 560
    assert chosen.count() == 550
    result, _ = apply_cdc(baseline, events, customers, products)
    assert baseline.count() == 99375
    assert result.count() == 99865
    assert result.select("order_id").distinct().count() == 99865

def test_replay_and_event_order(data):
    spark, baseline, customers, products = data
    events = generate_cdc_events(spark, baseline)
    chosen = latest_events(events)
    # The newer update wins over the older 11:00 AM update.
    assert chosen.filter(F.col("event_ts") == "2026-11-20T11:00:00").count() == 0
    once, _ = apply_cdc(baseline, events, customers, products)
    twice, _ = apply_cdc(once, events, customers, products)
    assert same_rows(once, twice)
