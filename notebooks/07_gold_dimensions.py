# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3a: Gold Layer — Dimension Tables
# MAGIC
# MAGIC **Purpose**: Build analytics-ready dimension tables for the star schema.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1 dim_customers — Unified Customer Dimension

# COMMAND ----------

dim_customers = (
    spark.table("silver.customers")
    .select(
        F.col("customer_id"),
        F.col("customer_email"),
        F.col("customer_name"),
        F.col("segment"),
        F.col("country"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code"),
        F.col("region"),
    )
    .withColumn("_etl_updated_at", F.current_timestamp())
)

dim_customers.write.mode("overwrite").saveAsTable("gold.dim_customers")
print(f"gold.dim_customers: {spark.table('gold.dim_customers').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.2 dim_products — Product Dimension

# COMMAND ----------

dim_products = (
    spark.table("silver.products")
    .select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("brand"),
        F.col("categories"),
        F.col("primary_category"),
        F.col("product_category_prefix"),
        F.col("colors"),
        F.col("sizes"),
        F.col("upc"),
        F.col("manufacturer"),
        F.col("dimension"),
        F.col("weight"),
        F.col("product_photos_qty"),
        F.col("date_added"),
        F.col("date_updated"),
    )
    .withColumn("_etl_updated_at", F.current_timestamp())
)

dim_products.write.mode("overwrite").saveAsTable("gold.dim_products")
print(f"gold.dim_products: {spark.table('gold.dim_products').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.3 dim_vendors — Vendor Dimension

# COMMAND ----------

dim_vendors = (
    spark.table("silver.vendors")
    .select("vendor_id", "vendor_name")
    .withColumn("_etl_updated_at", F.current_timestamp())
)

dim_vendors.write.mode("overwrite").saveAsTable("gold.dim_vendors")
print(f"gold.dim_vendors: {spark.table('gold.dim_vendors').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.4 dim_date — Date Dimension (Generated)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
import datetime

# Generate a date range covering all order dates
date_range = spark.sql("""
    SELECT explode(sequence(
        DATE('2014-01-01'),
        DATE('2025-12-31'),
        INTERVAL 1 DAY
    )) AS date
""")

dim_date = (
    date_range
    .withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("year", F.year(F.col("date")))
    .withColumn("quarter", F.quarter(F.col("date")))
    .withColumn("month", F.month(F.col("date")))
    .withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
    .withColumn("week_of_year", F.weekofyear(F.col("date")))
    .withColumn("day_of_month", F.dayofmonth(F.col("date")))
    .withColumn("day_of_week", F.dayofweek(F.col("date")))
    .withColumn("day_name", F.date_format(F.col("date"), "EEEE"))
    .withColumn("is_weekend",
        F.when(F.dayofweek(F.col("date")).isin(1, 7), True).otherwise(False)
    )
    # Fiscal calendar (assuming fiscal year starts April 1)
    .withColumn("fiscal_year",
        F.when(F.month(F.col("date")) >= 4, F.year(F.col("date")))
         .otherwise(F.year(F.col("date")) - 1)
    )
    .withColumn("fiscal_quarter",
        F.when(F.month(F.col("date")).isin(4, 5, 6), 1)
         .when(F.month(F.col("date")).isin(7, 8, 9), 2)
         .when(F.month(F.col("date")).isin(10, 11, 12), 3)
         .otherwise(4)
    )
    .withColumn("year_quarter", F.concat_ws("-Q", F.col("year"), F.col("quarter")))
)

dim_date.write.mode("overwrite").saveAsTable("gold.dim_date")
print(f"gold.dim_date: {spark.table('gold.dim_date').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.5 dim_geography — Geography Dimension

# COMMAND ----------

dim_geography = (
    spark.table("silver.customers")
    .select("city", "state", "region", "country", "postal_code")
    .distinct()
    .withColumn("geo_key",
        F.sha2(F.concat_ws("|", F.col("city"), F.col("state"), F.col("postal_code")), 256)
    )
    .withColumn("_etl_updated_at", F.current_timestamp())
)

dim_geography.write.mode("overwrite").saveAsTable("gold.dim_geography")
print(f"gold.dim_geography: {spark.table('gold.dim_geography').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("GOLD DIMENSIONS - SUMMARY")
print("=" * 60)
dims = ["gold.dim_customers", "gold.dim_products", "gold.dim_vendors", "gold.dim_date", "gold.dim_geography"]
for dim in dims:
    count = spark.table(dim).count()
    cols = len(spark.table(dim).columns)
    print(f"  {dim}: {count} rows, {cols} columns")
print("=" * 60)
