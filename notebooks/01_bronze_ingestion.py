# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest all 16 raw files from 6 regional systems into Delta tables in the Bronze layer.
# MAGIC Each table preserves the original data as-is with added lineage metadata columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Catalog & Schema Setup

# COMMAND ----------

# Create Unity Catalog and schemas for the Medallion Architecture
spark.sql("CREATE CATALOG IF NOT EXISTS globalmart_poc")
spark.sql("USE CATALOG globalmart_poc")

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

print("Catalog 'globalmart_poc' and schemas (bronze, silver, gold) created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Upload Raw Files to Volume
# MAGIC
# MAGIC Create a managed Volume under the bronze schema to hold raw source files.
# MAGIC Upload files manually via the Databricks UI or use the dbutils commands below.

# COMMAND ----------    

# Create a Volume for raw file storage
spark.sql("""
    CREATE VOLUME IF NOT EXISTS globalmart_poc.bronze.raw_files
""")

print("Volume 'globalmart_poc.bronze.raw_files' created.")
print(" Upload the GlobalMart_Retail_Data folder contents to:")
print("   /Volumes/globalmart_poc/bronze/raw_files/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Define Source File Manifest
# MAGIC
# MAGIC Maps every source file to its target Bronze table with metadata.

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, input_file_name

# Base path where raw files are uploaded in the Volume
VOLUME_BASE = "/Volumes/globalmart_poc/bronze/raw_files"

# Source file manifest — maps each file to its Bronze table
CSV_SOURCES = [
    # Customers
    {"file": f"{VOLUME_BASE}/Region_1/customers_1.csv", "table": "bronze.customers_region1", "region": "East"},
    {"file": f"{VOLUME_BASE}/Region_2/customers_2.csv", "table": "bronze.customers_region2", "region": "West"},
    {"file": f"{VOLUME_BASE}/Region_3/customers_3.csv", "table": "bronze.customers_region3", "region": "South"},
    {"file": f"{VOLUME_BASE}/Region_4/customers_4.csv", "table": "bronze.customers_region4", "region": "Central"},
    {"file": f"{VOLUME_BASE}/Region_5/customers_5.csv", "table": "bronze.customers_region5", "region": "North"},
    {"file": f"{VOLUME_BASE}/Region_6/customers_6.csv", "table": "bronze.customers_region6", "region": "Multi"},
    # Orders
    {"file": f"{VOLUME_BASE}/Region_1/orders_1.csv",    "table": "bronze.orders_region1",    "region": "East"},
    {"file": f"{VOLUME_BASE}/Region_3/orders_2.csv",    "table": "bronze.orders_region2",    "region": "South"},
    {"file": f"{VOLUME_BASE}/Region_5/orders_3.csv",    "table": "bronze.orders_region3",    "region": "North"},
    # Transactions
    {"file": f"{VOLUME_BASE}/Region_2/transactions_1.csv", "table": "bronze.transactions_region1", "region": "West"},
    {"file": f"{VOLUME_BASE}/Region_4/transactions_2.csv", "table": "bronze.transactions_region2", "region": "Central"},
    {"file": f"{VOLUME_BASE}/transactions_3.csv",          "table": "bronze.transactions_region3", "region": "Multi"},
    # Vendors
    {"file": f"{VOLUME_BASE}/vendors.csv",             "table": "bronze.vendors",            "region": "Global"},
]

JSON_SOURCES = [
    # Returns
    {"file": f"{VOLUME_BASE}/Region_6/returns_1.json", "table": "bronze.returns_1", "region": "Multi"},
    {"file": f"{VOLUME_BASE}/returns_2.json",          "table": "bronze.returns_2", "region": "Multi"},
    # Products
    {"file": f"{VOLUME_BASE}/products.json",           "table": "bronze.products",  "region": "Global"},
]

print(f" Manifest defined: {len(CSV_SOURCES)} CSV sources, {len(JSON_SOURCES)} JSON sources")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Ingest CSV Files into Bronze Delta Tables

# COMMAND ----------

for source in CSV_SOURCES:
    file_path = source["file"]
    table_name = source["table"]
    region = source["region"]

    print(f" Ingesting {file_path} → {table_name} ...")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .csv(file_path)
    )

    # Add lineage metadata columns
    df = (
        df
        .withColumn("_source_file", lit(file_path))
        .withColumn("_source_region", lit(region))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )

    # Write as Delta table (overwrite for idempotent re-runs)
    df.write.mode("overwrite").saveAsTable(table_name)

    row_count = spark.table(table_name).count()
    print(f"   {table_name}: {row_count} rows ingested")

print("\n All CSV files ingested into Bronze layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Ingest JSON Files into Bronze Delta Tables

# COMMAND ----------

for source in JSON_SOURCES:
    file_path = source["file"]
    table_name = source["table"]
    region = source["region"]

    print(f" Ingesting {file_path} → {table_name} ...")

    df = (
        spark.read
        .option("multiLine", "true")
        .json(file_path)
    )

    # Add lineage metadata columns
    df = (
        df
        .withColumn("_source_file", lit(file_path))
        .withColumn("_source_region", lit(region))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )

    # Write as Delta table
    df.write.mode("overwrite").saveAsTable(table_name)

    row_count = spark.table(table_name).count()
    print(f"   {table_name}: {row_count} rows ingested")

print("\n All JSON files ingested into Bronze layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Bronze Layer Validation

# COMMAND ----------

# Validate all Bronze tables exist and have expected row counts
EXPECTED_COUNTS = {
    "bronze.customers_region1": 87,
    "bronze.customers_region2": 112,
    "bronze.customers_region3": 53,
    "bronze.customers_region4": 80,
    "bronze.customers_region5": 42,
    "bronze.customers_region6": 374,
    "bronze.orders_region1": 2002,
    "bronze.orders_region2": 1752,
    "bronze.orders_region3": 1252,
    "bronze.transactions_region1": 3730,
    "bronze.transactions_region2": 3337,
    "bronze.transactions_region3": 2749,
    "bronze.returns_1": 500,
    "bronze.returns_2": 300,
    "bronze.products": 1774,
    "bronze.vendors": 7,
}

print("=" * 60)
print("BRONZE LAYER VALIDATION REPORT")
print("=" * 60)

all_passed = True
for table, expected in EXPECTED_COUNTS.items():
    try:
        actual = spark.table(table).count()
        # Check metadata columns exist
        cols = spark.table(table).columns
        has_meta = all(c in cols for c in ["_source_file", "_source_region", "_ingestion_timestamp"])
        status = "" if actual == expected and has_meta else ""
        if status == "":
            all_passed = False
        meta_status = "metadata" if has_meta else " missing metadata"
        print(f"  {status} {table}: {actual} rows (expected {expected}) | {meta_status}")
    except Exception as e:
        all_passed = False
        print(f"   {table}: ERROR - {str(e)}")

print("=" * 60)
print("ALL CHECKS PASSED" if all_passed else "SOME CHECKS FAILED — review above")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Bronze layer ingestion complete. All 16 raw files from 6 regional systems are now
# MAGIC stored as Delta tables in `globalmart_poc.bronze.*` with full lineage metadata.
# MAGIC
# MAGIC | Entity | Tables Created | Total Records |
# MAGIC |--------|---------------|---------------|
# MAGIC | Customers | 6 | ~748 |
# MAGIC | Orders | 3 | ~5,006 |
# MAGIC | Transactions | 3 | ~9,816 |
# MAGIC | Returns | 2 | 800 |
# MAGIC | Products | 1 | 1,774 |
# MAGIC | Vendors | 1 | 7 |
# MAGIC
# MAGIC **Next**: Run `02_silver_customers.py` to begin schema harmonization and data quality enforcement.
