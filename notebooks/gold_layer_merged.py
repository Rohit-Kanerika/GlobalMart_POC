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
print(f" gold.dim_customers: {spark.table('gold.dim_customers').count()} rows")

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
print(f" gold.dim_products: {spark.table('gold.dim_products').count()} rows")

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
print(f" gold.dim_vendors: {spark.table('gold.dim_vendors').count()} rows")

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
print(f" gold.dim_date: {spark.table('gold.dim_date').count()} rows")

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
print(f" gold.dim_geography: {spark.table('gold.dim_geography').count()} rows")

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
    print(f"   {dim}: {count} rows, {cols} columns")
print("=" * 60)


# COMMAND ----------

# MAGIC %md
# MAGIC --- 
# MAGIC 
# COMMAND ----------

# MAGIC %md
# MAGIC # Phase 3b: Gold Layer — Fact Tables
# MAGIC
# MAGIC **Purpose**: Build order and return fact tables by joining Silver layer entities.

# COMMAND ----------

from pyspark.sql import functions as F

# Database selected above

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.1 fact_orders — Order-Transaction Grain

# COMMAND ----------

# Join orders with transactions to create the order fact table
# Grain: one row per order-product combination (transaction level)

fact_orders = (
    spark.table("silver.transactions").alias("t")
    .join(
        spark.table("silver.orders").alias("o"),
        F.col("t.order_id") == F.col("o.order_id"),
        how="inner"
    )
    .select(
        # Fact keys
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("t.product_id"),
        F.col("o.vendor_id"),
        # Date keys (for joining to dim_date)
        F.date_format(F.col("o.order_purchase_date"), "yyyyMMdd").cast("int").alias("purchase_date_key"),
        # Order details
        F.col("o.ship_mode"),
        F.col("o.order_status"),
        F.col("o.order_purchase_date"),
        F.col("o.order_approved_at"),
        F.col("o.order_delivered_carrier_date"),
        F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"),
        # Transaction measures
        F.col("t.sales"),
        F.col("t.quantity"),
        F.col("t.discount"),
        F.col("t.profit"),
        F.col("t.payment_type"),
        F.col("t.payment_installments"),
        # Derived measures
        (F.col("t.sales") * (1 - F.coalesce(F.col("t.discount"), F.lit(0)))).alias("net_sales"),
        F.datediff(
            F.col("o.order_delivered_customer_date"),
            F.col("o.order_purchase_date")
        ).alias("delivery_days"),
        F.datediff(
            F.col("o.order_estimated_delivery_date"),
            F.col("o.order_delivered_customer_date")
        ).alias("delivery_variance_days"),  # positive = early, negative = late
    )
    .withColumn("_etl_updated_at", F.current_timestamp())
)

fact_orders.write.mode("overwrite").saveAsTable("gold.fact_orders")
count = spark.table("gold.fact_orders").count()
print(f" gold.fact_orders: {count} rows")

# COMMAND ----------

# Quick sanity check
print("Fact orders sample:")
spark.sql("""
    SELECT order_id, customer_id, product_id, sales, quantity, discount, profit, net_sales, delivery_days
    FROM gold.fact_orders
    LIMIT 5
""").show(truncate=False)

# Revenue totals
spark.sql("""
    SELECT
        ROUND(SUM(sales), 2) as total_gross_sales,
        ROUND(SUM(net_sales), 2) as total_net_sales,
        ROUND(SUM(profit), 2) as total_profit,
        COUNT(DISTINCT order_id) as unique_orders,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM gold.fact_orders
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.2 fact_returns — Returns Grain

# COMMAND ----------

# Join returns with orders and customers to create the return fact table

fact_returns = (
    spark.table("silver.returns").alias("r")
    .join(
        spark.table("silver.orders").alias("o"),
        F.col("r.order_id") == F.col("o.order_id"),
        how="left"
    )
    .join(
        spark.table("silver.customers").alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        how="left"
    )
    .select(
        # Keys
        F.col("r.order_id"),
        F.col("o.customer_id"),
        F.col("o.vendor_id"),
        # Return details
        F.col("r.return_reason"),
        F.col("r.return_date"),
        F.col("r.refund_amount"),
        F.col("r.return_status"),
        # Date keys
        F.date_format(F.col("r.return_date"), "yyyyMMdd").cast("int").alias("return_date_key"),
        # Customer context for fraud analysis
        F.col("c.customer_name"),
        F.col("c.segment"),
        F.col("c.region"),
        # Order context
        F.col("o.order_purchase_date"),
        # Derived: days between purchase and return
        F.datediff(F.col("r.return_date"), F.col("o.order_purchase_date")).alias("days_to_return"),
    )
    .withColumn("_etl_updated_at", F.current_timestamp())
)

fact_returns.write.mode("overwrite").saveAsTable("gold.fact_returns")
count = spark.table("gold.fact_returns").count()
print(f" gold.fact_returns: {count} rows")

# COMMAND ----------

# Quick sanity check
spark.sql("""
    SELECT return_reason, return_status, COUNT(*) as cnt, ROUND(SUM(refund_amount), 2) as total_refund
    FROM gold.fact_returns
    GROUP BY return_reason, return_status
    ORDER BY total_refund DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("GOLD FACT TABLES - SUMMARY")
print("=" * 60)
for tbl in ["gold.fact_orders", "gold.fact_returns"]:
    count = spark.table(tbl).count()
    cols = len(spark.table(tbl).columns)
    print(f"   {tbl}: {count} rows, {cols} columns")
print("=" * 60)


# COMMAND ----------

# MAGIC %md
# MAGIC --- 
# MAGIC 
# COMMAND ----------

# MAGIC %md
# MAGIC # Phase 3c: Gold Layer — Analytics Views
# MAGIC
# MAGIC **Purpose**: Pre-aggregated analytics views that directly address GlobalMart's 3 business problems:
# MAGIC 1. **Revenue Audit** — Revenue reconciliation with duplicate detection
# MAGIC 2. **Returns Fraud** — Customer return profiles with fraud risk scoring
# MAGIC 3. **Inventory Blindspot** — Product performance across regions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Database selected above

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.1 Revenue Reconciliation View
# MAGIC
# MAGIC Solves the **Revenue Audit Failure** problem:
# MAGIC - Revenue by region, quarter, customer
# MAGIC - Flags potential duplicate orders (same customer, same date, similar amount)

# COMMAND ----------

# Create the revenue reconciliation view
spark.sql("""
CREATE OR REPLACE VIEW gold.vw_revenue_reconciliation AS

WITH order_revenue AS (
    SELECT
        f.order_id,
        f.customer_id,
        c.customer_name,
        c.region,
        c.segment,
        f.order_purchase_date,
        d.year,
        d.quarter,
        d.year_quarter,
        d.fiscal_year,
        d.fiscal_quarter,
        ROUND(SUM(f.sales), 2) AS gross_revenue,
        ROUND(SUM(f.net_sales), 2) AS net_revenue,
        ROUND(SUM(f.profit), 2) AS total_profit,
        SUM(f.quantity) AS total_units
    FROM gold.fact_orders f
    JOIN gold.dim_customers c ON f.customer_id = c.customer_id
    LEFT JOIN gold.dim_date d ON f.purchase_date_key = d.date_key
    GROUP BY f.order_id, f.customer_id, c.customer_name, c.region, c.segment,
             f.order_purchase_date, d.year, d.quarter, d.year_quarter,
             d.fiscal_year, d.fiscal_quarter
),

-- Detect potential duplicate orders:
-- Same customer, same date, similar revenue (within 5%)
duplicate_flags AS (
    SELECT
        o1.order_id,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM order_revenue o2
                WHERE o2.customer_id = o1.customer_id
                  AND o2.order_id != o1.order_id
                  AND CAST(o2.order_purchase_date AS DATE) = CAST(o1.order_purchase_date AS DATE)
                  AND ABS(o2.gross_revenue - o1.gross_revenue) / NULLIF(o1.gross_revenue, 0) < 0.05
            )
            THEN TRUE
            ELSE FALSE
        END AS potential_duplicate_flag
    FROM order_revenue o1
)

SELECT
    r.*,
    COALESCE(df.potential_duplicate_flag, FALSE) AS potential_duplicate_flag
FROM order_revenue r
LEFT JOIN duplicate_flags df ON r.order_id = df.order_id
""")

print(" gold.vw_revenue_reconciliation created")

# Validate
total_rev = spark.sql("SELECT ROUND(SUM(gross_revenue),2) as total FROM gold.vw_revenue_reconciliation").collect()[0]["total"]
dup_count = spark.sql("SELECT COUNT(*) as cnt FROM gold.vw_revenue_reconciliation WHERE potential_duplicate_flag = TRUE").collect()[0]["cnt"]
print(f"   Total gross revenue: ${total_rev:,.2f}")
print(f"   Potential duplicate orders: {dup_count}")

# COMMAND ----------

# Regional revenue summary
spark.sql("""
    SELECT
        region,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT customer_id) AS customers,
        ROUND(SUM(gross_revenue), 2) AS gross_revenue,
        ROUND(SUM(net_revenue), 2) AS net_revenue,
        ROUND(SUM(total_profit), 2) AS total_profit,
        SUM(CASE WHEN potential_duplicate_flag THEN 1 ELSE 0 END) AS flagged_duplicates
    FROM gold.vw_revenue_reconciliation
    GROUP BY region
    ORDER BY gross_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.2 Customer Return Profile View
# MAGIC
# MAGIC Solves the **Returns Fraud Exposure** problem:
# MAGIC - Unified return history per customer across ALL regions
# MAGIC - Fraud risk scoring based on frequency and amounts

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_customer_return_profile AS

WITH return_history AS (
    SELECT
        r.customer_id,
        c.customer_name,
        c.region AS home_region,
        c.segment,
        c.customer_email,
        COUNT(*) AS total_returns,
        COUNT(CASE WHEN r.return_status = 'Approved' THEN 1 END) AS approved_returns,
        COUNT(CASE WHEN r.return_status = 'Rejected' THEN 1 END) AS rejected_returns,
        COUNT(CASE WHEN r.return_status = 'Pending' THEN 1 END) AS pending_returns,
        ROUND(SUM(r.refund_amount), 2) AS total_refund_amount,
        ROUND(AVG(r.refund_amount), 2) AS avg_refund_amount,
        ROUND(MAX(r.refund_amount), 2) AS max_refund_amount,
        MIN(r.return_date) AS first_return_date,
        MAX(r.return_date) AS last_return_date,
        DATEDIFF(MAX(r.return_date), MIN(r.return_date)) AS return_span_days,
        COLLECT_SET(r.return_reason) AS return_reasons,
        COUNT(DISTINCT r.return_reason) AS unique_reasons
    FROM gold.fact_returns r
    LEFT JOIN gold.dim_customers c ON r.customer_id = c.customer_id
    WHERE r.customer_id IS NOT NULL
    GROUP BY r.customer_id, c.customer_name, c.region, c.segment, c.customer_email
)

SELECT
    *,
    -- Fraud Risk Score (1-5)
    CASE
        -- HIGH RISK: >5 returns OR >$1000 total refunds in < 120 days
        WHEN (total_returns > 5 AND return_span_days < 120)
          OR total_refund_amount > 1000
        THEN 5

        -- ELEVATED: >3 returns in < 120 days OR >$500 total refunds
        WHEN (total_returns > 3 AND return_span_days < 120)
          OR total_refund_amount > 500
        THEN 4

        -- MODERATE: >3 total returns OR avg refund > $200
        WHEN total_returns > 3
          OR avg_refund_amount > 200
        THEN 3

        -- LOW-MODERATE: 2-3 returns
        WHEN total_returns >= 2
        THEN 2

        -- LOW: single return
        ELSE 1
    END AS fraud_risk_score,

    CASE
        WHEN (total_returns > 5 AND return_span_days < 120) OR total_refund_amount > 1000
        THEN 'HIGH - Immediate Review Required'
        WHEN (total_returns > 3 AND return_span_days < 120) OR total_refund_amount > 500
        THEN 'ELEVATED - Manual Verification Needed'
        WHEN total_returns > 3 OR avg_refund_amount > 200
        THEN 'MODERATE - Monitor'
        WHEN total_returns >= 2
        THEN 'LOW-MODERATE - Watch'
        ELSE 'LOW - Normal'
    END AS fraud_risk_label
FROM return_history
""")

print(" gold.vw_customer_return_profile created")

# Show high-risk customers
spark.sql("""
    SELECT customer_id, customer_name, home_region, total_returns,
           total_refund_amount, fraud_risk_score, fraud_risk_label
    FROM gold.vw_customer_return_profile
    WHERE fraud_risk_score >= 4
    ORDER BY fraud_risk_score DESC, total_refund_amount DESC
""").show(20, truncate=False)

# COMMAND ----------

# Fraud risk distribution
spark.sql("""
    SELECT fraud_risk_label, COUNT(*) AS customers, ROUND(SUM(total_refund_amount), 2) AS total_refunds
    FROM gold.vw_customer_return_profile
    GROUP BY fraud_risk_label
    ORDER BY customers DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.3 Inventory Performance View
# MAGIC
# MAGIC Solves the **Inventory Blindspot** problem:
# MAGIC - Sales velocity by product × region
# MAGIC - Identifies products selling in one region but not another

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_inventory_performance AS

WITH product_region_sales AS (
    SELECT
        f.product_id,
        p.product_name,
        p.brand,
        p.primary_category,
        c.region,
        COUNT(DISTINCT f.order_id) AS order_count,
        SUM(f.quantity) AS total_units_sold,
        ROUND(SUM(f.sales), 2) AS total_revenue,
        ROUND(SUM(f.profit), 2) AS total_profit,
        MIN(f.order_purchase_date) AS first_sale_date,
        MAX(f.order_purchase_date) AS last_sale_date,
        DATEDIFF(MAX(f.order_purchase_date), MIN(f.order_purchase_date)) AS sales_span_days,
        -- Approximate weekly velocity
        ROUND(
            SUM(f.quantity) / NULLIF(DATEDIFF(MAX(f.order_purchase_date), MIN(f.order_purchase_date)) / 7.0, 0),
            2
        ) AS weekly_unit_velocity
    FROM gold.fact_orders f
    JOIN gold.dim_products p ON f.product_id = p.product_id
    JOIN gold.dim_customers c ON f.customer_id = c.customer_id
    GROUP BY f.product_id, p.product_name, p.brand, p.primary_category, c.region
),

-- Count in how many regions each product sells
product_region_coverage AS (
    SELECT
        product_id,
        COUNT(DISTINCT region) AS regions_with_sales,
        COLLECT_SET(region) AS selling_regions,
        ROUND(SUM(total_revenue), 2) AS global_revenue,
        SUM(total_units_sold) AS global_units
    FROM product_region_sales
    GROUP BY product_id
),

-- All possible product-region combinations
all_regions AS (SELECT DISTINCT region FROM gold.dim_customers),
all_products AS (SELECT DISTINCT product_id FROM product_region_sales),
all_combos AS (
    SELECT p.product_id, r.region
    FROM all_products p
    CROSS JOIN all_regions r
)

SELECT
    prs.*,
    prc.regions_with_sales,
    prc.selling_regions,
    prc.global_revenue,
    prc.global_units,
    -- Flag: product sells in other regions but NOT this one
    CASE
        WHEN prs.product_id IS NULL AND prc.regions_with_sales >= 1
        THEN TRUE
        ELSE FALSE
    END AS inventory_blindspot_flag,
    -- Performance category
    CASE
        WHEN prs.weekly_unit_velocity IS NULL OR prs.weekly_unit_velocity = 0 THEN 'No Sales'
        WHEN prs.weekly_unit_velocity >= 5 THEN 'Fast Mover'
        WHEN prs.weekly_unit_velocity >= 1 THEN 'Moderate'
        ELSE 'Slow Mover'
    END AS velocity_category
FROM all_combos ac
LEFT JOIN product_region_sales prs ON ac.product_id = prs.product_id AND ac.region = prs.region
LEFT JOIN product_region_coverage prc ON ac.product_id = prc.product_id
WHERE prc.product_id IS NOT NULL  -- Only products that sell somewhere
""")

print(" gold.vw_inventory_performance created")

# COMMAND ----------

# Products with inventory blindspots (selling somewhere but not everywhere)
spark.sql("""
    SELECT product_id, product_name, region, velocity_category,
           total_units_sold, total_revenue, regions_with_sales, selling_regions
    FROM gold.vw_inventory_performance
    WHERE total_units_sold IS NOT NULL
    ORDER BY total_revenue DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# Velocity distribution by region
spark.sql("""
    SELECT region, velocity_category, COUNT(*) AS products, ROUND(SUM(COALESCE(total_revenue, 0)), 2) AS revenue
    FROM gold.vw_inventory_performance
    GROUP BY region, velocity_category
    ORDER BY region, revenue DESC
""").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Analytics Views

# COMMAND ----------

print("=" * 70)
print("GOLD ANALYTICS VIEWS - SUMMARY")
print("=" * 70)

# Revenue Reconciliation
rev = spark.table("gold.vw_revenue_reconciliation")
print(f"\n vw_revenue_reconciliation:")
print(f"   Rows: {rev.count()}")
print(f"   Potential duplicates: {rev.filter(F.col('potential_duplicate_flag') == True).count()}")

# Customer Return Profile
ret = spark.table("gold.vw_customer_return_profile")
high_risk = ret.filter(F.col("fraud_risk_score") >= 4).count()
print(f"\n vw_customer_return_profile:")
print(f"   Customers with returns: {ret.count()}")
print(f"   High/Elevated risk:     {high_risk}")

# Inventory Performance
inv = spark.table("gold.vw_inventory_performance")
print(f"\n vw_inventory_performance:")
print(f"   Product-region entries: {inv.count()}")
blindspots = inv.filter(F.col("inventory_blindspot_flag") == True).count()
print(f"   Inventory blindspots:   {blindspots}")

print("\n" + "=" * 70)
print(" All 3 business problems now have analytics-ready views.")
print("=" * 70)


# COMMAND ----------

# MAGIC %md
# MAGIC --- 
# MAGIC 
# COMMAND ----------

