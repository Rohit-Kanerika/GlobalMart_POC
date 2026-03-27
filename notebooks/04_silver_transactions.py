# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2c: Silver Layer — Transactions Data Harmonization
# MAGIC
# MAGIC **Purpose**: Unify 3 regional transaction files, clean currency/percentage formatting,
# MAGIC and handle missing columns (profit absent in transactions_region2).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import DataFrame
from functools import reduce

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Schema Harmonization & Union

# COMMAND ----------

CANONICAL_TXN_COLUMNS = [
    "order_id", "product_id", "sales", "quantity", "discount", "profit",
    "payment_type", "payment_installments",
    "_source_file", "_source_region", "_ingestion_timestamp"
]

txn_tables = [
    "bronze.transactions_region1",  # Has: Order_id, Product_id, Sales, Quantity, discount, profit, payment_type, payment_installments
    "bronze.transactions_region2",  # Has: Order_id, Product_id, Sales, Quantity, discount, payment_type, payment_installments — MISSING profit
    "bronze.transactions_region3",  # Has: Order_ID, Product_ID, Sales, Quantity, discount, profit, payment_type, payment_installments
]

txn_dfs = []
for table in txn_tables:
    df = spark.table(table)

    # Normalize column names to lowercase
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    # Handle missing profit column (transactions_region2)
    if "profit" not in df.columns:
        df = df.withColumn("profit", F.lit(None).cast(StringType()))

    # Force cast metric/numeric fields to STRING before union to prevent Spark from
    # throwing CAST_INVALID_INPUT during union type coercion (e.g. string & double -> double)
    for col_to_str in ["sales", "discount", "profit", "quantity", "payment_installments"]:
        if col_to_str in df.columns:
            df = df.withColumn(col_to_str, F.col(col_to_str).cast(StringType()))

    df = df.select(*CANONICAL_TXN_COLUMNS)
    txn_dfs.append(df)
    print(f"Read {table}: {df.count()} rows")

txn_raw = txn_dfs[0]
for df in txn_dfs[1:]:
    txn_raw = txn_raw.unionByName(df)
print(f"\nTotal raw transactions: {txn_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Value Cleaning
# MAGIC
# MAGIC Issues to fix:
# MAGIC - `Sales` has `$` prefix on some values (e.g., "$22.25")
# MAGIC - `discount` has `%` suffix on some values (e.g., "40%") — needs conversion to decimal (0.4)
# MAGIC - `profit` has `$` prefix on some values

# COMMAND ----------

def clean_currency(col_name):
    """Strip $ and try_cast to DOUBLE (returns NULL for malformed values)."""
    return F.expr(f"try_cast(regexp_replace({col_name}, '[\\$,]', '') as double)").alias(col_name)

def clean_discount(col_name):
    """Convert percentage strings (e.g. '40%') to decimal (0.4), pass through already-decimal values.
    Uses try_cast to safely return NULL for any malformed input instead of raising CAST_INVALID_INPUT.
    """
    raw = F.col(col_name)
    return (
        F.when(
            raw.contains("%"),
            # Strip % then try_cast — avoids CAST_INVALID_INPUT on Photon/serverless
            (F.expr(f"try_cast(regexp_replace({col_name}, '%', '') as double)") / 100.0)
        )
        .otherwise(
            F.expr(f"try_cast({col_name} as double)")
        )
    ).alias(col_name)

txn_cleaned = (
    txn_raw
    .withColumn("sales", clean_currency("sales"))
    .withColumn("profit", clean_currency("profit"))
    .withColumn("discount", clean_discount("discount"))
    .withColumn("quantity", F.expr("try_cast(quantity as int)"))
    .withColumn("payment_installments", F.expr("try_cast(payment_installments as int)"))
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
)

print("Currency and percentage values cleaned.")
txn_cleaned.select("sales", "discount", "profit").describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Data Quality Checks & Quarantine

# COMMAND ----------

txn_with_dq = (
    txn_cleaned
    .withColumn("_dq_order_id_null", F.col("order_id").isNull())
    .withColumn("_dq_product_id_null", F.col("product_id").isNull())
    .withColumn("_dq_sales_invalid", F.col("sales").isNull() | (F.col("sales") < 0))
    .withColumn("_dq_quantity_invalid", F.col("quantity").isNull() | (F.col("quantity") <= 0))
    .withColumn("_dq_discount_invalid",
        F.col("discount").isNotNull() & ((F.col("discount") < 0) | (F.col("discount") > 1)))
    # Referential integrity: order_id -> silver.orders
    .join(
        spark.table("silver.orders").select("order_id").distinct().withColumn("_order_exists", F.lit(True)),
        on="order_id", how="left"
    )
    .withColumn("_dq_order_not_found", F.col("_order_exists").isNull())
    # Referential integrity: product_id -> bronze.products (silver.products may not exist yet)
    .withColumn(
        "_dq_failed",
        F.col("_dq_order_id_null")
        | F.col("_dq_product_id_null")
        | F.col("_dq_sales_invalid")
        | F.col("_dq_quantity_invalid")
        | F.col("_dq_discount_invalid")
    )
    .withColumn(
        "_dq_failure_reasons",
        F.concat_ws(", ",
            F.when(F.col("_dq_order_id_null"), F.lit("order_id is NULL")),
            F.when(F.col("_dq_product_id_null"), F.lit("product_id is NULL")),
            F.when(F.col("_dq_sales_invalid"), F.lit("invalid sales value")),
            F.when(F.col("_dq_quantity_invalid"), F.lit("invalid quantity")),
            F.when(F.col("_dq_discount_invalid"), F.concat(F.lit("discount out of range: "), F.col("discount").cast(StringType()))),
            F.when(F.col("_dq_order_not_found"), F.lit("order_id not in silver.orders (warning)")),
        )
    )
)

dq_cols = [c for c in txn_with_dq.columns if c.startswith("_dq_") or c == "_order_exists"]

txn_clean = txn_with_dq.filter(~F.col("_dq_failed")).drop(*dq_cols)
txn_quarantine = (
    txn_with_dq
    .filter(F.col("_dq_failed"))
    .withColumn("_quarantine_timestamp", F.current_timestamp())
    .select(*CANONICAL_TXN_COLUMNS, "_dq_failure_reasons", "_quarantine_timestamp")
)

clean_count = txn_clean.count()
quarantine_count = txn_quarantine.count()
print(f"Clean transactions:      {clean_count}")
print(f"Quarantined transactions: {quarantine_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Save Silver Transactions

# COMMAND ----------

txn_clean.write.mode("overwrite").saveAsTable("silver.transactions")
txn_quarantine.write.mode("overwrite").saveAsTable("silver.transactions_quarantine")

print(f" silver.transactions: {spark.table('silver.transactions').count()} rows")
print(f" silver.transactions_quarantine: {spark.table('silver.transactions_quarantine').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("SILVER TRANSACTIONS - VALIDATION REPORT")
print("=" * 60)
print(f"  Input records:       {txn_raw.count()}")
print(f"  Clean records:       {clean_count}")
print(f"  Quarantined records: {quarantine_count}")
print()

# Check no $ or % remain
dollar_check = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions WHERE CAST(sales AS STRING) LIKE '%$%'").collect()[0]["cnt"]
pct_check = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions WHERE CAST(discount AS STRING) LIKE '%%'").collect()[0]["cnt"]
print(f"  Records with '$' in sales: {dollar_check}")
print(f"  Discount range: 0-1 enforced")

# Profit NULL check (expected for region 2 transactions)
null_profit = spark.sql("SELECT COUNT(*) as cnt FROM silver.transactions WHERE profit IS NULL").collect()[0]["cnt"]
print(f"  Records with NULL profit (from Region 2): {null_profit}")

# Payment type distribution
spark.sql("""
    SELECT payment_type, COUNT(*) as cnt
    FROM silver.transactions
    GROUP BY payment_type
    ORDER BY cnt DESC
""").show()
print("=" * 60)
