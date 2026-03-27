# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2d: Silver Layer — Returns Data Harmonization
# MAGIC
# MAGIC **Purpose**: Unify 2 JSON returns files with different schemas and status codes.
# MAGIC Addresses the **Returns Fraud** problem by creating a unified return history.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, DateType
from pyspark.sql import DataFrame
from functools import reduce

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Schema Harmonization
# MAGIC
# MAGIC | returns_1 columns | returns_2 columns | Canonical |
# MAGIC |---|---|---|
# MAGIC | order_id | OrderId | order_id |
# MAGIC | return_reason | reason | return_reason |
# MAGIC | return_date | date_of_return | return_date |
# MAGIC | refund_amount | amount | refund_amount |
# MAGIC | return_status | status | return_status |

# COMMAND ----------

CANONICAL_RETURN_COLUMNS = [
    "order_id", "return_reason", "return_date", "refund_amount", "return_status",
    "_source_file", "_source_region", "_ingestion_timestamp"
]

RETURN_COLUMN_MAPS = {
    "bronze.returns_1": {
        # Already canonical names
    },
    "bronze.returns_2": {
        "OrderId": "order_id",
        "reason": "return_reason",
        "date_of_return": "return_date",
        "amount": "refund_amount",
        "status": "return_status",
    },
}

return_dfs = []
for table, col_map in RETURN_COLUMN_MAPS.items():
    df = spark.table(table)
    for old_name, new_name in col_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    # Select canonical columns and ensure return_date and refund_amount are strings
    # This prevents implicit cast failures during unionByName if Bronze schemas differ
    select_exprs = [
        F.col(c).cast(StringType()).alias(c) if c in ["return_date", "refund_amount"] else F.col(c)
        for c in CANONICAL_RETURN_COLUMNS
    ]
    df = df.select(*select_exprs)
    
    return_dfs.append(df)
    print(f"Read {table}: {df.count()} rows")

returns_raw = return_dfs[0]
for df in return_dfs[1:]:
    returns_raw = returns_raw.unionByName(df)
print(f"\nTotal raw returns: {returns_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Value Cleaning & Standardization

# COMMAND ----------

# Status standardization mapping
STATUS_MAP = {
    "RJCTD": "Rejected",
    "Rejected": "Rejected",
    "APRVD": "Approved",
    "Approved": "Approved",
    "PNDG": "Pending",
    "Pending": "Pending",
}

status_expr = F.create_map([F.lit(x) for pair in STATUS_MAP.items() for x in pair])

returns_cleaned = (
    returns_raw
    # Clean refund amount: strip $ and try_cast to DOUBLE to tolerate malformed inputs (e.g. '?')
    .withColumn("refund_amount",
        F.expr("try_cast(regexp_replace(cast(refund_amount as string), '[\\$,]', '') as double)")
    )
    # Standardize return status
    .withColumn("return_status",
        F.coalesce(status_expr[F.trim(F.col("return_status"))], F.trim(F.col("return_status")))
    )
    # Parse dates — normalize to ISO format first, then try_cast (Photon-safe)
    # Step 1: ensure string type
    .withColumn("return_date", F.col("return_date").cast("string"))
    # Step 2: normalize MM/DD/YYYY or DD-MM-YYYY patterns to YYYY-MM-DD
    .withColumn("return_date",
        F.when(
            F.col("return_date").rlike(r"^\d{2}/\d{2}/\d{4}"),
            F.regexp_replace(F.col("return_date"), r"^(\d{2})/(\d{2})/(\d{4})(.*)", "$3-$1-$2$4")
        ).when(
            F.col("return_date").rlike(r"^\d{2}-\d{2}-\d{4}"),
            F.regexp_replace(F.col("return_date"), r"^(\d{2})-(\d{2})-(\d{4})(.*)", "$3-$1-$2$4")
        ).otherwise(F.col("return_date"))
    )
    # Step 3: safe cast to date
    .withColumn("return_date", F.expr("try_cast(return_date as date)"))
    # Trim string fields
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("return_reason", F.initcap(F.trim(F.col("return_reason"))))
)

print("Values cleaned and standardized.")
returns_cleaned.select("return_status").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Data Quality Checks & Quarantine

# COMMAND ----------

VALID_STATUSES = ["Approved", "Rejected", "Pending"]
VALID_REASONS = ["Not Satisfied", "Wrong Delivery", "Defective Product", "Late Delivery", "Other"]

returns_with_dq = (
    returns_cleaned
    .withColumn("_dq_order_id_null", F.col("order_id").isNull())
    .withColumn("_dq_refund_invalid", F.col("refund_amount").isNull() | (F.col("refund_amount") <= 0))
    .withColumn("_dq_status_invalid", ~F.col("return_status").isin(VALID_STATUSES))
    .withColumn("_dq_date_null", F.col("return_date").isNull())
    # Referential integrity: order_id -> silver.orders (cast to string to prevent implicit type coercion errors)
    .join(
        spark.table("silver.orders").select(F.col("order_id").cast("string")).distinct().withColumn("_order_exists", F.lit(True)),
        on="order_id", how="left"
    )
    .withColumn("_dq_order_not_found", F.col("_order_exists").isNull())
    .withColumn(
        "_dq_failed",
        F.col("_dq_order_id_null")
        | F.col("_dq_refund_invalid")
        | F.col("_dq_status_invalid")
    )
    .withColumn(
        "_dq_failure_reasons",
        F.concat_ws(", ",
            F.when(F.col("_dq_order_id_null"), F.lit("order_id is NULL")),
            F.when(F.col("_dq_refund_invalid"), F.lit("invalid refund_amount")),
            F.when(F.col("_dq_status_invalid"), F.concat(F.lit("invalid status: "), F.coalesce(F.col("return_status"), F.lit("NULL")))),
            F.when(F.col("_dq_date_null"), F.lit("return_date is NULL (warning)")),
            F.when(F.col("_dq_order_not_found"), F.lit("order_id not in silver.orders (warning)")),
        )
    )
)

dq_cols = [c for c in returns_with_dq.columns if c.startswith("_dq_") or c == "_order_exists"]

returns_clean = returns_with_dq.filter(~F.col("_dq_failed")).drop(*dq_cols)
returns_quarantine = (
    returns_with_dq
    .filter(F.col("_dq_failed"))
    .withColumn("_quarantine_timestamp", F.current_timestamp())
    .select(*CANONICAL_RETURN_COLUMNS, "_dq_failure_reasons", "_quarantine_timestamp")
)

clean_count = returns_clean.count()
quarantine_count = returns_quarantine.count()
print(f"Clean returns:      {clean_count}")
print(f"Quarantined returns: {quarantine_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Save Silver Returns

# COMMAND ----------

returns_clean.write.mode("overwrite").saveAsTable("silver.returns")
returns_quarantine.write.mode("overwrite").saveAsTable("silver.returns_quarantine")

print(f" silver.returns: {spark.table('silver.returns').count()} rows")
print(f" silver.returns_quarantine: {spark.table('silver.returns_quarantine').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("SILVER RETURNS - VALIDATION REPORT")
print("=" * 60)
print(f"  Input records:       {returns_raw.count()}")
print(f"  Clean records:       {clean_count}")
print(f"  Quarantined records: {quarantine_count}")
print()

# Status distribution
spark.sql("""
    SELECT return_status, COUNT(*) as cnt
    FROM silver.returns
    GROUP BY return_status
    ORDER BY cnt DESC
""").show()

# Reason distribution
spark.sql("""
    SELECT return_reason, COUNT(*) as cnt
    FROM silver.returns
    GROUP BY return_reason
    ORDER BY cnt DESC
""").show()

# Referential integrity
orphan_returns = spark.sql("""
    SELECT COUNT(*) as cnt FROM silver.returns r
    LEFT JOIN silver.orders o ON r.order_id = o.order_id
    WHERE o.order_id IS NULL
""").collect()[0]["cnt"]
print(f"  Returns with no matching order: {orphan_returns}")
print("=" * 60)
