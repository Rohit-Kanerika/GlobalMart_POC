# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2b: Silver Layer — Orders Data Harmonization
# MAGIC
# MAGIC **Purpose**: Unify 3 regional order files with standardized schemas, dates, and ship modes.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql import DataFrame
from functools import reduce

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Schema Harmonization & Union

# COMMAND ----------

# Canonical schema for orders
CANONICAL_ORDER_COLUMNS = [
    "order_id", "customer_id", "vendor_id", "ship_mode", "order_status",
    "order_purchase_date", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "_source_file", "_source_region", "_ingestion_timestamp"
]

# Read and harmonize each orders table
# orders_region1: MM/DD/YYYY HH:MM format — has all columns
# orders_region2: YYYY-MM-DD H:MM format, abbreviated ship modes — has all columns
# orders_region3: YYYY-MM-DD H:MM format — MISSING order_estimated_delivery_date

order_tables = ["bronze.orders_region1", "bronze.orders_region2", "bronze.orders_region3"]
order_dfs = []

DATE_COLUMNS = [
    "order_purchase_date", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for table in order_tables:
    df = spark.table(table)

    # Add missing columns if needed
    if "order_estimated_delivery_date" not in df.columns:
        df = df.withColumn("order_estimated_delivery_date", F.lit(None).cast(StringType()))

    # Select canonical columns and ensure all date columns are strings
    # This prevents implicit cast failures during unionByName if Bronze schemas differ
    select_exprs = [
        F.col(c).cast(StringType()).alias(c) if c in DATE_COLUMNS else F.col(c)
        for c in CANONICAL_ORDER_COLUMNS
    ]
    df = df.select(*select_exprs)
    order_dfs.append(df)
    print(f"Read {table}: {df.count()} rows, columns: {len(df.columns)}")

orders_raw = order_dfs[0]
for df in order_dfs[1:]:
    orders_raw = orders_raw.unionByName(df)
print(f"\nTotal raw orders: {orders_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Date Parsing & Standardization
# MAGIC
# MAGIC Input formats:
# MAGIC - `MM/DD/YYYY HH:MM` (Region 1)
# MAGIC - `YYYY-MM-DD H:MM` (Regions 2 & 3)
# MAGIC - `YYYY-MM-DD` (some estimated delivery dates)

# COMMAND ----------

# Parse dates using a 2-step approach for Photon/serverless compatibility:
# Step 1: Normalize all date strings to ISO format using regexp_replace (pure string ops)
# Step 2: Use SQL try_cast to safely convert to TIMESTAMP (returns NULL, never throws)

orders_parsed = orders_raw
for date_col in DATE_COLUMNS:
    # Ensure column is string type first
    orders_parsed = orders_parsed.withColumn(date_col, F.col(date_col).cast("string"))

    # Normalize: MM/DD/YYYY... → YYYY-MM-DD...
    # Regex captures: group1=month, group2=day, group3=year, group4=rest (time part)
    orders_parsed = orders_parsed.withColumn(
        date_col,
        F.when(
            F.col(date_col).rlike(r"^\d{2}/\d{2}/\d{4}"),
            F.regexp_replace(F.col(date_col), r"^(\d{2})/(\d{2})/(\d{4})(.*)", "$3-$1-$2$4")
        ).otherwise(F.col(date_col))
    )

    # Now all dates are ISO-ish (YYYY-MM-DD or YYYY-MM-DD H:MM) → safe try_cast
    orders_parsed = orders_parsed.withColumn(
        date_col,
        F.expr(f"try_cast(`{date_col}` as timestamp)")
    )

print("Dates parsed to TIMESTAMP type.")
orders_parsed.select(*DATE_COLUMNS).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Ship Mode Standardization

# COMMAND ----------

SHIP_MODE_MAP = {
    "1st Class": "First Class",
    "Std Class": "Standard Class",
    "2nd Class": "Second Class",
    "First Class": "First Class",
    "Standard Class": "Standard Class",
    "Second Class": "Second Class",
    "Same Day": "Same Day",
}

ship_mode_expr = F.create_map([F.lit(x) for pair in SHIP_MODE_MAP.items() for x in pair])

orders_standardized = (
    orders_parsed
    .withColumn("ship_mode",
        F.when(F.col("ship_mode").isNull() | (F.trim(F.col("ship_mode")) == ""), F.lit("Unknown"))
         .otherwise(ship_mode_expr[F.trim(F.col("ship_mode"))])
    )
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("vendor_id", F.trim(F.col("vendor_id")))
)

print("Ship modes standardized:")
orders_standardized.select("ship_mode").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Data Quality Checks & Quarantine

# COMMAND ----------

VALID_SHIP_MODES = ["First Class", "Second Class", "Standard Class", "Same Day", "Unknown"]

orders_with_dq = (
    orders_standardized
    .withColumn("_dq_order_id_null", F.col("order_id").isNull())
    .withColumn("_dq_customer_id_null", F.col("customer_id").isNull())
    .withColumn("_dq_purchase_date_null", F.col("order_purchase_date").isNull())
    .withColumn("_dq_ship_mode_invalid", ~F.col("ship_mode").isin(VALID_SHIP_MODES))
    # Chronological order check: purchase <= approved <= carrier <= delivered
    .withColumn("_dq_date_order_violation",
        (F.col("order_approved_at").isNotNull() & (F.col("order_purchase_date") > F.col("order_approved_at"))) |
        (F.col("order_delivered_carrier_date").isNotNull() & F.col("order_approved_at").isNotNull() &
         (F.col("order_approved_at") > F.col("order_delivered_carrier_date")))
    )
    # Check referential integrity with silver.customers
    .join(
        spark.table("silver.customers").select("customer_id").distinct().withColumn("_customer_exists", F.lit(True)),
        on="customer_id",
        how="left"
    )
    .withColumn("_dq_customer_not_found", F.col("_customer_exists").isNull())
    .withColumn(
        "_dq_failed",
        F.col("_dq_order_id_null")
        | F.col("_dq_customer_id_null")
        | F.col("_dq_purchase_date_null")
        | F.col("_dq_ship_mode_invalid")
    )
    .withColumn(
        "_dq_failure_reasons",
        F.concat_ws(", ",
            F.when(F.col("_dq_order_id_null"), F.lit("order_id is NULL")),
            F.when(F.col("_dq_customer_id_null"), F.lit("customer_id is NULL")),
            F.when(F.col("_dq_purchase_date_null"), F.lit("purchase_date is NULL")),
            F.when(F.col("_dq_ship_mode_invalid"), F.concat(F.lit("invalid ship_mode: "), F.coalesce(F.col("ship_mode"), F.lit("NULL")))),
            F.when(F.col("_dq_customer_not_found"), F.lit("customer_id not in silver.customers (warning)")),
            F.when(F.col("_dq_date_order_violation"), F.lit("date chronology violation (warning)")),
        )
    )
)

# Split clean vs quarantine (only hard failures go to quarantine)
dq_cols = [c for c in orders_with_dq.columns if c.startswith("_dq_") or c == "_customer_exists"]

orders_clean = orders_with_dq.filter(~F.col("_dq_failed")).drop(*dq_cols)
orders_quarantine = (
    orders_with_dq
    .filter(F.col("_dq_failed"))
    .withColumn("_quarantine_timestamp", F.current_timestamp())
    .select(*CANONICAL_ORDER_COLUMNS, "_dq_failure_reasons", "_quarantine_timestamp")
)

clean_count = orders_clean.count()
quarantine_count = orders_quarantine.count()
print(f"Clean orders:      {clean_count}")
print(f"Quarantined orders: {quarantine_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Save Silver Orders

# COMMAND ----------

orders_clean.write.mode("overwrite").saveAsTable("silver.orders")
orders_quarantine.write.mode("overwrite").saveAsTable("silver.orders_quarantine")

print(f" silver.orders: {spark.table('silver.orders').count()} rows")
print(f" silver.orders_quarantine: {spark.table('silver.orders_quarantine').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("SILVER ORDERS - VALIDATION REPORT")
print("=" * 60)
print(f"  Input records (all regions):  {orders_raw.count()}")
print(f"  Clean records:                {clean_count}")
print(f"  Quarantined records:          {quarantine_count}")
print()

# Date parsing check
null_dates = spark.table("silver.orders").filter(F.col("order_purchase_date").isNull()).count()
print(f"  Null purchase dates: {null_dates}")

# Ship mode check
spark.sql("""
    SELECT ship_mode, COUNT(*) as cnt
    FROM silver.orders
    GROUP BY ship_mode
    ORDER BY cnt DESC
""").show()

# Referential integrity warning
orphan_orders = spark.sql("""
    SELECT COUNT(*) as cnt FROM silver.orders o
    LEFT JOIN silver.customers c ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").collect()[0]["cnt"]
print(f"  Orders with no matching customer: {orphan_orders}")
print("=" * 60)
