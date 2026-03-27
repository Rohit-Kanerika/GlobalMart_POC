# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2e: Silver Layer — Products & Vendors
# MAGIC
# MAGIC **Purpose**: Clean product catalog and pass through vendor reference data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, LongType

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Products — Clean & Standardize

# COMMAND ----------

products_raw = spark.table("bronze.products")
print(f"Raw products: {products_raw.count()} rows")
print(f"Columns: {products_raw.columns}")

# COMMAND ----------

# Clean product data
products_clean = (
    products_raw
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("brand", F.initcap(F.trim(F.col("brand"))))
    .withColumn("categories", F.trim(F.col("categories")))
    .withColumn("colors", F.initcap(F.trim(F.col("colors"))))
    # Fix UPC from scientific notation (e.g., "6.40E+11") to full number
    # Use try_cast to tolerate malformed inputs like embedded URLs
    .withColumn("upc",
        F.when(
            F.col("upc").isNotNull(),
            F.expr("cast(cast(try_cast(upc as double) as bigint) as string)")
        )
    )
    # Parse date fields
    .withColumn("date_added", F.to_timestamp(F.col("dateAdded"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("date_updated", F.to_timestamp(F.col("dateUpdated"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .drop("dateAdded", "dateUpdated")
    # Extract primary category from comma-separated categories
    .withColumn("primary_category",
        F.split(F.col("categories"), ",").getItem(0)
    )
    # Product category hierarchy from product_id prefix
    .withColumn("product_category_prefix",
        F.split(F.col("product_id"), "-").getItem(0)
    )
)

# COMMAND ----------

# DQ checks for products
products_with_dq = (
    products_clean
    .withColumn("_dq_product_id_null", F.col("product_id").isNull())
    .withColumn("_dq_product_name_null", F.col("product_name").isNull())
    .withColumn("_dq_failed",
        F.col("_dq_product_id_null") | F.col("_dq_product_name_null")
    )
)

products_final = products_with_dq.filter(~F.col("_dq_failed")).drop(
    "_dq_product_id_null", "_dq_product_name_null", "_dq_failed"
)

products_quarantine = (
    products_with_dq
    .filter(F.col("_dq_failed"))
    .withColumn("_quarantine_timestamp", F.current_timestamp())
)

products_final.write.mode("overwrite").saveAsTable("silver.products")
if products_quarantine.count() > 0:
    products_quarantine.write.mode("overwrite").saveAsTable("silver.products_quarantine")

final_count = spark.table("silver.products").count()
print(f" silver.products: {final_count} rows")
print(f"Quarantined products: {products_quarantine.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Vendors — Pass Through

# COMMAND ----------

vendors = spark.table("bronze.vendors")

# Vendors data is already clean — just standardize
vendors_clean = (
    vendors
    .withColumn("vendor_id", F.trim(F.col("vendor_id")))
    .withColumn("vendor_name", F.trim(F.col("vendor_name")))
)

vendors_clean.write.mode("overwrite").saveAsTable("silver.vendors")
print(f" silver.vendors: {spark.table('silver.vendors').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("SILVER PRODUCTS & VENDORS - VALIDATION REPORT")
print("=" * 60)
print(f"  silver.products: {spark.table('silver.products').count()} rows")
print(f"  silver.vendors:  {spark.table('silver.vendors').count()} rows")
print()

# Null field analysis for products
for col_name in ["dimension", "manufacturer", "weight"]:
    null_count = spark.table("silver.products").filter(F.col(col_name).isNull()).count()
    total = spark.table("silver.products").count()
    print(f"  {col_name} NULL rate: {null_count}/{total} ({100*null_count/total:.1f}%)")

print()
print("  Product category prefix distribution:")
spark.sql("""
    SELECT product_category_prefix, COUNT(*) as cnt
    FROM silver.products
    GROUP BY product_category_prefix
    ORDER BY cnt DESC
""").show()

print("  Vendor list:")
spark.sql("SELECT * FROM silver.vendors").show()
print("=" * 60)
