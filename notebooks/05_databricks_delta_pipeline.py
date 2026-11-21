# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — E-commerce Lakehouse: S3/Volume → Bronze → Silver → Gold → Delta MERGE
# MAGIC Run in Databricks with Unity Catalog enabled. Upload the 20 order shards,
# MAGIC customers.csv and products.csv into a UC volume's `raw/` directory.
# MAGIC No AWS credentials or bucket names are stored in this notebook.
# MAGIC This is a demo, not a live CDC connector.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "ecommerce_raw", "Volume")
dbutils.widgets.text("raw_subdir", "raw", "Directory inside volume")
import re
from pyspark.sql import functions as F, types as T, Window
def safe_identifier(s):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", s):
        raise ValueError("Unsafe identifier: " + s)
    return s
CAT = safe_identifier(dbutils.widgets.get("catalog"))
SCH = safe_identifier(dbutils.widgets.get("schema"))
VOL = safe_identifier(dbutils.widgets.get("volume"))
SUBDIR = dbutils.widgets.get("raw_subdir").strip("/")
if SUBDIR and not re.fullmatch(r"[A-Za-z0-9_/-]+", SUBDIR):
    raise ValueError("Invalid directory")
RAW = f"/Volumes/{CAT}/{SCH}/{VOL}/" + (SUBDIR + "/" if SUBDIR else "")
PREFIX = f"`{CAT}`.`{SCH}`"
BRONZE = PREFIX + ".orders_bronze"
SILVER = PREFIX + ".orders_silver"
GOLD = PREFIX + ".revenue_by_category"
print("Raw input volume:", RAW)
print("Delta table schema:", PREFIX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Ingest Bronze
# MAGIC Read the CSV shards as source data. A production ingestion pipeline should
# MAGIC use Auto Loader or COPY INTO with checkpoints; this demo loads the static source
# MAGIC on the first run and leaves the table intact on subsequent runs.

# COMMAND ----------

schema = T.StructType([
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
if not spark.catalog.tableExists(BRONZE.replace("`","")):
    orders_raw = spark.read.option("header", True).schema(schema).csv(RAW + "orders_part_*.csv")
    assert orders_raw.count() == 100100, "Upload all 20 order CSV shards"
    orders_raw.write.format("delta").mode("errorifexists").saveAsTable(BRONZE)
raw = spark.table(BRONZE)
print("Bronze rows:", raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Build clean Silver Delta table once
# MAGIC Deduplicate, cast types, join dimensions, quarantine invalid orders through a
# MAGIC reject count, and recognize revenue only for completed orders.

# COMMAND ----------

if not spark.catalog.tableExists(SILVER.replace("`","")):
    customers = spark.read.option("header", True).csv(RAW + "customers.csv")
    products = spark.read.option("header", True).csv(RAW + "products.csv")
    w = Window.partitionBy("order_id").orderBy("customer_id", "product_id", "order_date")
    unique = (raw.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))
    typed = (unique.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
             .withColumn("quantity", F.col("quantity").cast("int"))
             .withColumn("unit_price", F.col("unit_price").cast("decimal(14,2)"))
             .withColumn("discount_pct", F.col("discount_pct").cast("decimal(6,4)")))
    joined = (typed.join(F.broadcast(customers.select("customer_id","state","acquisition_channel")),
                         "customer_id", "left")
              .join(F.broadcast(products.select("product_id","product_name","category")),
                    "product_id", "left"))
    valid = (F.col("order_id").isNotNull() & F.col("state").isNotNull()
             & F.col("category").isNotNull() & F.col("order_date").isNotNull()
             & (F.col("quantity") > 0) & (F.col("unit_price") >= 0)
             & F.col("discount_pct").between(0,1)
             & F.col("status").isin("completed","returned","cancelled"))
    clean = (joined.filter(valid)
             .withColumn("gross_amount", F.round(F.col("quantity")*F.col("unit_price"),2))
             .withColumn("net_amount", F.round(F.col("gross_amount")*(1-F.col("discount_pct")),2))
             .withColumn("recognized_revenue",
                         F.when(F.col("status")=="completed",F.col("net_amount"))
                          .otherwise(F.lit(0).cast("decimal(18,2)")))
             .withColumn("order_month",F.date_format("order_date","yyyy-MM")))
    assert clean.count() == 99375, "Unexpected Silver row count"
    clean.write.format("delta").mode("errorifexists").saveAsTable(SILVER)
print("Current Silver rows:", spark.table(SILVER).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Simulated change events
# MAGIC We create 500 new orders, 40 replacements and 10 deletions.
# MAGIC All values are deterministic so re-running this notebook has the same effect.
# MAGIC Real CDC events should come from log-based sources and carry sequence versions.

# COMMAND ----------

current = spark.table(SILVER)
columns = current.columns
# Reuse 500 stable existing orders as templates for brand-new synthetic IDs.
templates = current.filter(
    (F.col("order_id") >= F.lit("O0001001")) &
    (F.col("order_id") <= F.lit("O0001500"))
)
inserts = (templates.withColumn("order_id",
                F.format_string("O%07d", F.substring("order_id",2,7).cast("int")+100000))
           .withColumn("order_date",F.to_date(F.lit("2026-11-20")))
           .withColumn("order_month",F.lit("2026-11"))
           .withColumn("status",F.lit("completed"))
           .withColumn("recognized_revenue",F.col("net_amount"))
           .withColumn("op",F.lit("I"))
           .withColumn("event_ts",F.lit("2026-11-20T12:00:00")))
updates = (current.filter(
    (F.col("order_id") >= F.lit("O0000626")) &
    (F.col("order_id") <= F.lit("O0000665")))
    .withColumn("quantity",F.lit(7))
    .withColumn("gross_amount",F.round(F.col("unit_price")*F.col("quantity"),2))
    .withColumn("net_amount",F.round(F.col("gross_amount")*(1-F.col("discount_pct")),2))
    .withColumn("recognized_revenue",
                F.when(F.col("status")=="completed",F.col("net_amount"))
                 .otherwise(F.lit(0).cast("decimal(18,2)")))
    .withColumn("op",F.lit("U"))
    .withColumn("event_ts",F.lit("2026-11-20T12:00:00")))
deletes = (current.filter(
    (F.col("order_id") >= F.lit("O0000666")) &
    (F.col("order_id") <= F.lit("O0000675")))
    .withColumn("op",F.lit("D"))
    .withColumn("event_ts",F.lit("2026-11-20T12:00:00")))
# Stale updates are intentionally lower-priority events.
stale = (updates.limit(10)
         .withColumn("quantity",F.lit(9))
         .withColumn("event_ts",F.lit("2026-11-20T11:00:00")))
events = inserts.unionByName(updates).unionByName(deletes).unionByName(stale)
events = events.select(*columns,"op","event_ts")
assert events.count() == 560, "Unexpected CDC event count"
w = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())
latest = (events.withColumn("_rn",F.row_number().over(w))
          .filter(F.col("_rn")==1).drop("_rn"))
assert latest.count() == 550
latest.createOrReplaceTempView("ecommerce_cdc_latest")
print("Effective change events:",latest.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Transactional Delta MERGE
# MAGIC Unlike writing plain Parquet folders, Delta MERGE atomically applies changes
# MAGIC to the target table. Repeated events with identical target values are safe
# MAGIC for this demo. The real production system must track per-key event versions.

# COMMAND ----------

cols_quoted = [f"`{c}`" for c in columns]
insert_names = ", ".join(cols_quoted)
insert_values = ", ".join(f"s.`{c}`" for c in columns)
update_assign = ", ".join(f"t.`{c}` = s.`{c}`" for c in columns if c!="order_id")
spark.sql(f"""
MERGE INTO {SILVER} AS t
USING ecommerce_cdc_latest AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET {update_assign}
WHEN NOT MATCHED AND s.op = 'I' THEN INSERT ({insert_names}) VALUES ({insert_values})
""")
assert spark.table(SILVER).count() == 99865
assert spark.table(SILVER).select("order_id").distinct().count() == 99865
print("After Delta MERGE:",spark.table(SILVER).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Gold analytics + reconciliation
# MAGIC Gold is rebuilt from the current Silver snapshot for simplicity. A future
# MAGIC version can incrementally update only affected reporting partitions.

# COMMAND ----------

gold = (spark.table(SILVER).groupBy("category")
        .agg(F.count("*").alias("orders"),
             F.round(F.sum("recognized_revenue"),2).alias("revenue")))
gold.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(GOLD)
display(spark.table(GOLD).orderBy(F.desc("revenue")))
assert spark.table(GOLD).agg(F.sum("orders")).first()[0] == 99865
print("Phase 5 Delta Lake pipeline complete; Gold reconciles to Silver ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Production checklist
# MAGIC - Configure an S3-backed Unity Catalog external **volume** or upload CSVs to a managed volume
# MAGIC - Never commit AWS secrets or access keys
# MAGIC - Schedule the job with Databricks Workflows only after validating the inputs
# MAGIC - Add Auto Loader, durable checkpoints, and per-key sequence numbers for real CDC
# MAGIC - Use a service principal, least-privilege Unity Catalog grants, alerting, and budgets
# MAGIC - Note: the first run is a one-time full load; subsequent runs reuse existing tables
