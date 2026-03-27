# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2a: Silver Layer — Customer Data Harmonization & Identity Resolution
# MAGIC
# MAGIC **Purpose**: Unify 6 regional customer files into a single, deduplicated customer master.
# MAGIC Addresses the **Revenue Audit** problem by resolving overlapping customer identities.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Schema Harmonization
# MAGIC
# MAGIC Each regional file uses different column names. We map them all to a canonical schema:
# MAGIC ```
# MAGIC customer_id, customer_email, customer_name, segment, country, city, state, postal_code, region, _source_file, _source_region, _ingestion_timestamp
# MAGIC ```

# COMMAND ----------

# Define column mappings per region
# Format: {source_column: canonical_column}
COLUMN_MAPS = {
    "customers_region1": {
        # Already uses canonical names — no renaming needed
    },
    "customers_region2": {
        "CustomerID": "customer_id",
    },
    "customers_region3": {
        "cust_id": "customer_id",
    },
    "customers_region4": {
        # customer_id is correct, but city/state are SWAPPED in the source header
        # Source header: customer_id,customer_email,customer_name,segment,country,state,city,postal_code,region
        # The column order has state before city — need to swap them
    },
    "customers_region5": {
        # Missing customer_email entirely — will add as NULL
    },
    "customers_region6": {
        "customer_identifier": "customer_id",
        "email_address": "customer_email",
        "full_name": "customer_name",
        "customer_segment": "segment",
    },
}

# Canonical column order
CANONICAL_COLUMNS = [
    "customer_id", "customer_email", "customer_name", "segment",
    "country", "city", "state", "postal_code", "region",
    "_source_file", "_source_region", "_ingestion_timestamp"
]


def harmonize_customers(table_name: str, column_map: dict) -> DataFrame:
    """Read a Bronze customer table and harmonize it to canonical schema."""
    df = spark.table(f"bronze.{table_name}")

    # Apply column renames
    for old_name, new_name in column_map.items():
        df = df.withColumnRenamed(old_name, new_name)

    # Handle Region 4: city and state are swapped in the source header
    if table_name == "customers_region4":
        df = (df
              .withColumnRenamed("city", "_temp_city")
              .withColumnRenamed("state", "city")
              .withColumnRenamed("_temp_city", "state"))

    # Handle Region 5: missing customer_email column
    if "customer_email" not in df.columns:
        df = df.withColumn("customer_email", F.lit(None).cast(StringType()))

    # Select only canonical columns (in order)
    df = df.select(*CANONICAL_COLUMNS)

    return df


# Harmonize all 6 regional customer tables
customer_dfs = []
for table_name, col_map in COLUMN_MAPS.items():
    df = harmonize_customers(table_name, col_map)
    customer_dfs.append(df)
    print(f"Harmonized {table_name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Union All Regions & Standardize Values

# COMMAND ----------

from functools import reduce

# Union all harmonized customer DataFrames
# Using explicit loop instead of reduce(DataFrame.unionByName, ...) for serverless compatibility
customers_raw = customer_dfs[0]
for df in customer_dfs[1:]:
    customers_raw = customers_raw.unionByName(df)

print(f"Total raw customer records after union: {customers_raw.count()}")

# COMMAND ----------

# Standardize segment values
SEGMENT_MAP = {
    "CONS": "Consumer",
    "CORP": "Corporate",
    "HO": "Home Office",
    "Consumer": "Consumer",
    "Corporate": "Corporate",
    "Home Office": "Home Office",
}

# Standardize region values
REGION_MAP = {
    "W": "West",
    "E": "East",
    "S": "South",
    "N": "North",
    "West": "West",
    "East": "East",
    "South": "South",
    "North": "North",
    "Central": "Central",
}

# Build CASE WHEN expressions for mapping
segment_expr = F.create_map([F.lit(x) for pair in SEGMENT_MAP.items() for x in pair])
region_expr = F.create_map([F.lit(x) for pair in REGION_MAP.items() for x in pair])

customers_standardized = (
    customers_raw
    .withColumn("segment", segment_expr[F.col("segment")])
    .withColumn("region", region_expr[F.col("region")])
    .withColumn("customer_email", F.lower(F.trim(F.col("customer_email"))))
    .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .withColumn("country", F.initcap(F.trim(F.col("country"))))
    .withColumn("postal_code", F.trim(F.col("postal_code")))
)

print("Segment and region values standardized.")
customers_standardized.select("segment").distinct().show()
customers_standardized.select("region").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Data Quality Checks & Quarantine

# COMMAND ----------

VALID_SEGMENTS = ["Consumer", "Corporate", "Home Office"]
VALID_REGIONS = ["East", "West", "Central", "South", "North"]

# Define quality rules
quality_checks = (
    customers_standardized
    .withColumn("_dq_customer_id_null", F.col("customer_id").isNull())
    .withColumn("_dq_customer_name_null", F.col("customer_name").isNull())
    .withColumn("_dq_segment_invalid", ~F.col("segment").isin(VALID_SEGMENTS))
    .withColumn("_dq_region_invalid", ~F.col("region").isin(VALID_REGIONS))
    .withColumn(
        "_dq_failed",
        F.col("_dq_customer_id_null")
        | F.col("_dq_customer_name_null")
        | F.col("_dq_segment_invalid")
        | F.col("_dq_region_invalid")
    )
    .withColumn(
        "_dq_failure_reasons",
        F.concat_ws(", ",
            F.when(F.col("_dq_customer_id_null"), F.lit("customer_id is NULL")),
            F.when(F.col("_dq_customer_name_null"), F.lit("customer_name is NULL")),
            F.when(F.col("_dq_segment_invalid"), F.concat(F.lit("invalid segment: "), F.coalesce(F.col("segment"), F.lit("NULL")))),
            F.when(F.col("_dq_region_invalid"), F.concat(F.lit("invalid region: "), F.coalesce(F.col("region"), F.lit("NULL")))),
        )
    )
)

# Split into clean and quarantine
customers_clean = quality_checks.filter(~F.col("_dq_failed")).drop(
    "_dq_customer_id_null", "_dq_customer_name_null", "_dq_segment_invalid",
    "_dq_region_invalid", "_dq_failed", "_dq_failure_reasons"
)

customers_quarantine = (
    quality_checks
    .filter(F.col("_dq_failed"))
    .withColumn("_quarantine_timestamp", F.current_timestamp())
    .select(
        *CANONICAL_COLUMNS,
        "_dq_failure_reasons",
        "_quarantine_timestamp"
    )
)

clean_count = customers_clean.count()
quarantine_count = customers_quarantine.count()

print(f"Clean records:      {clean_count}")
print(f"Quarantined records: {quarantine_count}")

# Save quarantine table
customers_quarantine.write.mode("overwrite").saveAsTable("silver.customers_quarantine")
print(" Quarantine table saved: silver.customers_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Customer Identity Resolution & Deduplication
# MAGIC
# MAGIC Resolves the Revenue Audit problem by creating a single canonical customer ID.
# MAGIC Strategy:
# MAGIC 1. Use `customer_id` as primary match key
# MAGIC 2. Use `customer_email` as secondary match to catch same-person/different-ID cases
# MAGIC 3. Build a cross-reference table mapping all regional occurrences to a canonical ID

# COMMAND ----------

from pyspark.sql.window import Window

# Step 1: Deduplicate by customer_id — take the most recent record per customer_id
window_by_id = Window.partitionBy("customer_id").orderBy(F.col("_ingestion_timestamp").desc())

customers_deduped = (
    customers_clean
    .withColumn("_row_num", F.row_number().over(window_by_id))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

print(f"After customer_id dedup: {customers_deduped.count()} unique customers")

# COMMAND ----------

# Step 2: Build cross-reference table
# Track all (customer_id, source_region, source_file) combinations

customer_xref = (
    customers_clean
    .select("customer_id", "customer_email", "customer_name", "_source_region", "_source_file")
    .distinct()
    .withColumn("_created_timestamp", F.current_timestamp())
)

customer_xref.write.mode("overwrite").saveAsTable("silver.customer_id_xref")
xref_count = spark.table("silver.customer_id_xref").count()
print(f"Cross-reference table created: silver.customer_id_xref ({xref_count} mappings)")

# COMMAND ----------

# Step 3: Email-based identity linking
# Find cases where the same email is associated with different customer_ids

email_dupes = (
    customers_deduped
    .filter(F.col("customer_email").isNotNull())
    .groupBy("customer_email")
    .agg(
        F.count("*").alias("id_count"),
        F.collect_set("customer_id").alias("linked_customer_ids")
    )
    .filter(F.col("id_count") > 1)
)

email_dupe_count = email_dupes.count()
print(f" Email-based duplicates found: {email_dupe_count} emails linked to multiple customer IDs")

if email_dupe_count > 0:
    email_dupes.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Save Unified Customer Master

# COMMAND ----------

# Write the final deduplicated customer master
customers_deduped.write.mode("overwrite").saveAsTable("silver.customers")

final_count = spark.table("silver.customers").count()
print(f" Unified customer master saved: silver.customers ({final_count} customers)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("SILVER CUSTOMERS - VALIDATION REPORT")
print("=" * 60)
print(f"  Input records (all regions):      {customers_raw.count()}")
print(f"  Quarantined (DQ failures):        {quarantine_count}")
print(f"  Clean records:                    {clean_count}")
print(f"  Deduplicated customer master:     {final_count}")
print(f"  Cross-reference mappings:         {xref_count}")
print(f"  Email-based identity links:       {email_dupe_count}")
print("=" * 60)

# Schema verification
cols = spark.table("silver.customers").columns
expected = set(CANONICAL_COLUMNS)
actual = set(cols)
if expected == actual:
    print("  Schema matches canonical definition")
else:
    print(f"  Schema mismatch — missing: {expected - actual}, extra: {actual - expected}")

# Value verification
invalid_segments = spark.table("silver.customers").filter(~F.col("segment").isin(VALID_SEGMENTS)).count()
invalid_regions = spark.table("silver.customers").filter(~F.col("region").isin(VALID_REGIONS)).count()
print(f"  Invalid segments: {invalid_segments}")
print(f"  Invalid regions:  {invalid_regions}")
print("=" * 60)
