"""Phase 2: PySpark batch ETL. Bronze CSV -> validated Silver Parquet -> Gold Parquet."""
from pathlib import Path
import argparse
import json
from pyspark.sql import SparkSession, Window, functions as F, types as T

ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("order_date", T.StringType()),
    T.StructField("quantity", T.StringType()),
    T.StructField("unit_price", T.StringType()),
    T.StructField("discount_pct", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("payment_method", T.StringType()),
])
VALID_STATUSES = ["completed", "returned", "cancelled"]

def spark_session():
    return (SparkSession.builder.master("local[*]")
            .appName("EcommerceBronzeSilverGold")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())

def extract(spark, raw_dir):
    root = Path(raw_dir)
    shards = sorted(root.glob("orders_part_*.csv"))
    if not shards:
        raise FileNotFoundError("No orders_part_*.csv files found in " + str(root))
    orders = spark.read.option("header", True).schema(ORDER_SCHEMA).csv([str(p) for p in shards])
    customers = spark.read.option("header", True).csv(str(root / "customers.csv"))
    products = spark.read.option("header", True).csv(str(root / "products.csv"))
    return orders, customers, products

def transform(orders, customers, products):
    # Deterministic deduplication: duplicates in this dataset are identical; sorting
    # still makes the winner well-defined if later data differs.
    w = Window.partitionBy("order_id").orderBy("customer_id", "product_id", "order_date")
    unique = (orders.withColumn("_row_number", F.row_number().over(w))
              .filter(F.col("_row_number") == 1).drop("_row_number"))
    typed = (unique
             .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
             .withColumn("quantity", F.col("quantity").cast("int"))
             .withColumn("unit_price", F.col("unit_price").cast("decimal(14,2)"))
             .withColumn("discount_pct", F.col("discount_pct").cast("decimal(6,4)")))
    customers = customers.select("customer_id", "state", "acquisition_channel").dropDuplicates(["customer_id"])
    products = products.select("product_id", "product_name", "category").dropDuplicates(["product_id"])
    enriched = (typed.join(F.broadcast(customers), "customer_id", "left")
                .join(F.broadcast(products), "product_id", "left"))
    reasons = F.array(
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
        F.when(F.col("state").isNull(), F.lit("unknown_customer")),
        F.when(F.col("category").isNull(), F.lit("unknown_product")),
        F.when(F.col("order_date").isNull(), F.lit("invalid_date")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid_price")),
        F.when(F.col("discount_pct").isNull() | ~F.col("discount_pct").between(0, 1), F.lit("invalid_discount")),
        F.when(F.col("status").isNull() | ~F.col("status").isin(*VALID_STATUSES), F.lit("invalid_status")),
    )
    validated = enriched.withColumn("reject_reasons", F.array_compact(reasons))
    rejected = validated.filter(F.size("reject_reasons") > 0)
    silver = (validated.filter(F.size("reject_reasons") == 0).drop("reject_reasons")
              .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
              .withColumn("net_amount", F.round(F.col("gross_amount") * (F.lit(1) - F.col("discount_pct")), 2))
              .withColumn("recognized_revenue", F.when(F.col("status") == "completed", F.col("net_amount"))
                          .otherwise(F.lit(0).cast("decimal(18,2)")))
              .withColumn("order_month", F.date_format("order_date", "yyyy-MM")))
    return unique, silver, rejected

def gold_tables(silver):
    metrics = [F.count("*").alias("orders"),
               F.round(F.sum("recognized_revenue"), 2).alias("revenue")]
    return {
        "monthly_revenue": silver.groupBy("order_month").agg(*metrics).orderBy("order_month"),
        "category_revenue": silver.groupBy("category").agg(*metrics).orderBy("category"),
        "state_revenue": silver.groupBy("state").agg(*metrics).orderBy("state"),
    }

def run(spark, raw_dir, output_dir):
    output_dir = Path(output_dir)
    raw_orders, customers, products = extract(spark, raw_dir)
    unique, silver, rejected = transform(raw_orders, customers, products)
    raw_count = raw_orders.count()
    unique_count = unique.count()
    silver_count = silver.count()
    rejected_count = rejected.count()
    quality = {
        "raw_rows": raw_count,
        "unique_orders": unique_count,
        "duplicates_removed": raw_count - unique_count,
        "accepted_rows": silver_count,
        "rejected_rows": rejected_count,
        "all_rows_accounted_for": silver_count + rejected_count == unique_count,
    }
    assert quality["all_rows_accounted_for"], quality
    assert (raw_count, unique_count, silver_count, rejected_count) == (100100, 100000, 99375, 625), quality
    assert silver.select("order_id").distinct().count() == silver_count
    assert silver.filter((F.col("status") != "completed") & (F.col("recognized_revenue") != 0)).count() == 0

    silver.write.mode("overwrite").parquet(str(output_dir / "silver" / "orders"))
    rejected.write.mode("overwrite").parquet(str(output_dir / "quarantine" / "orders"))
    tables = gold_tables(silver)
    for name, frame in tables.items():
        frame.write.mode("overwrite").parquet(str(output_dir / "gold" / name))
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "quality_report_spark.json").write_text(json.dumps(quality, indent=2))
    return quality, tables

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--output-dir", default="outputs/spark")
    args = parser.parse_args()
    spark = spark_session()
    try:
        quality, _ = run(spark, args.raw_dir, args.output_dir)
        print(json.dumps(quality, indent=2))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
