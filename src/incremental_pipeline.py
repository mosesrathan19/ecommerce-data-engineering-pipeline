"""Phase 3: simulated CDC and idempotent incremental Parquet snapshots.

Run after Phase 2 or independently. Source CSVs remain unchanged.
"""
from pathlib import Path
import json
from pyspark.sql import Window, functions as F, types as T
from src.spark_pipeline import transform, gold_tables

EVENT_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("order_date", T.StringType()),
    T.StructField("quantity", T.StringType()),
    T.StructField("unit_price", T.StringType()),
    T.StructField("discount_pct", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("op", T.StringType(), False),
    T.StructField("event_ts", T.StringType(), False),
])
RAW_COLUMNS = ["order_id", "customer_id", "product_id", "order_date",
               "quantity", "unit_price", "discount_pct", "status", "payment_method"]
BATCH_ID = "2026-11-20-demo"


def generate_cdc_events(spark, baseline):
    """Create a deterministic synthetic CDC feed: 500 inserts, 40 updates,
    10 deletes, and 10 older update events = 560 raw events.
    """
    baseline_rows = (baseline.orderBy("order_id").limit(50)
                     .select(*RAW_COLUMNS).collect())
    new_orders = []
    for i in range(1, 501):
        new_orders.append((
            f"O{100000+i:07d}", f"C{1+i%5000:05d}", f"P{1+i%200:04d}",
            "2026-11-20", str(1 + i%5), "49.99", "0.05",
            "completed", "card", "I", "2026-11-20T12:00:00"))
    def event(row, operation, quantity_delta=0, timestamp="2026-11-20T12:00:00"):
        values = [str(row[c]) if row[c] is not None else None for c in RAW_COLUMNS]
        if operation == "U":
            values[4] = str(int(row["quantity"]) + quantity_delta)
        return tuple(values + [operation, timestamp])
    updates = [event(r, "U", 1) for r in baseline_rows[:40]]
    deletes = [event(r, "D") for r in baseline_rows[40:50]]
    stale_updates = [event(r, "U", 3, "2026-11-20T11:00:00")
                     for r in baseline_rows[:10]]
    return spark.createDataFrame(new_orders + updates + deletes + stale_updates,
                                 schema=EVENT_SCHEMA)


def latest_events(events):
    """Use event time for last-write-wins; reject ambiguous same-time events."""
    invalid = events.filter(
        F.col("order_id").isNull() | F.col("event_ts").isNull()
        | (~F.col("op").isin("I", "U", "D"))
        | F.to_timestamp("event_ts").isNull())
    if invalid.limit(1).count():
        raise ValueError("CDC feed contains invalid IDs, operations, or timestamps")
    collisions = events.groupBy("order_id", "event_ts").count().filter(F.col("count") > 1)
    if collisions.limit(1).count():
        raise ValueError("Ambiguous order_id + event_ts in CDC feed")
    w = Window.partitionBy("order_id").orderBy(F.to_timestamp("event_ts").desc())
    return (events.withColumn("_rank", F.row_number().over(w))
            .filter(F.col("_rank") == 1).drop("_rank"))


def apply_cdc(base, events, customers, products):
    """Implement keyed insert/update/delete without append-only duplication."""
    chosen = latest_events(events)
    incoming = chosen.filter(F.col("op").isin("I", "U")).select(*RAW_COLUMNS)
    _, upserts, rejected = transform(incoming, customers, products)
    if rejected.limit(1).count():
        raise ValueError("CDC upserts failed Silver quality checks")
    unaffected = base.join(chosen.select("order_id"), "order_id", "left_anti")
    merged = unaffected.unionByName(upserts.select(*base.columns))
    return merged, chosen


def same_rows(a, b):
    """Test exact multiset equality, not just row-count equality."""
    b = b.select(*a.columns)
    return (a.exceptAll(b).limit(1).count() == 0
            and b.exceptAll(a).limit(1).count() == 0)


def run_demo(spark, root, output_root, batch_id=BATCH_ID):
    """Persist immutable Parquet snapshot and a completed-batch manifest."""
    root, output_root = Path(root), Path(output_root)
    baseline_path = output_root / "baseline" / "orders"
    batch_dir = output_root / "snapshots" / batch_id
    final_path = batch_dir / "orders"
    manifest = batch_dir / "manifest.json"

    if manifest.exists():
        saved = json.loads(manifest.read_text())
        return saved, spark.read.parquet(str(final_path))

    if not baseline_path.exists():
        from src.spark_pipeline import run as run_full
        phase2 = output_root / "initial_full_refresh"
        run_full(spark, root / "data/raw", phase2)
        spark.read.parquet(str(phase2 / "silver" / "orders")).write.mode(
            "errorifexists").parquet(str(baseline_path))

    base = spark.read.parquet(str(baseline_path))
    customers = spark.read.option("header", True).csv(
        str(root / "data/raw/customers.csv"))
    products = spark.read.option("header", True).csv(
        str(root / "data/raw/products.csv"))
    events = generate_cdc_events(spark, base)
    merged, chosen = apply_cdc(base, events, customers, products)
    counts = {
        "base_rows": base.count(),
        "incoming_events": events.count(),
        "effective_events": chosen.count(),
        "final_rows": merged.count(),
    }
    assert counts == {"base_rows": 99375, "incoming_events": 560,
                      "effective_events": 550, "final_rows": 99865}, counts
    assert merged.select("order_id").distinct().count() == counts["final_rows"]
    assert same_rows(merged, apply_cdc(merged, events, customers, products)[0])

    # Write new version to a NEW path, never overwrite a Parquet input being read.
    if final_path.exists():
        raise FileExistsError("Incomplete batch snapshot exists; inspect before retry")
    merged.write.mode("errorifexists").parquet(str(final_path))
    verified = spark.read.parquet(str(final_path))
    assert same_rows(verified, merged)
    for name, frame in gold_tables(verified).items():
        frame.write.mode("errorifexists").parquet(str(batch_dir / "gold" / name))
    manifest.write_text(json.dumps({
        "batch_id": batch_id, **counts,
        "replay_is_idempotent": True,
        "snapshot_path": str(final_path),
    }, indent=2))
    return json.loads(manifest.read_text()), verified
