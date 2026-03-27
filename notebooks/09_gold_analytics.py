# Databricks notebook source
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

spark.sql("USE CATALOG globalmart_poc")

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

print("gold.vw_revenue_reconciliation created")

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

print("gold.vw_customer_return_profile created")

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

print("gold.vw_inventory_performance created")

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
print(f"\nvw_revenue_reconciliation:")
print(f"   Rows: {rev.count()}")
print(f"   Potential duplicates: {rev.filter(F.col('potential_duplicate_flag') == True).count()}")

# Customer Return Profile
ret = spark.table("gold.vw_customer_return_profile")
high_risk = ret.filter(F.col("fraud_risk_score") >= 4).count()
print(f"\nvw_customer_return_profile:")
print(f"   Customers with returns: {ret.count()}")
print(f"   High/Elevated risk:     {high_risk}")

# Inventory Performance
inv = spark.table("gold.vw_inventory_performance")
print(f"\nvw_inventory_performance:")
print(f"   Product-region entries: {inv.count()}")
blindspots = inv.filter(F.col("inventory_blindspot_flag") == True).count()
print(f"   Inventory blindspots:   {blindspots}")

print("\n" + "=" * 70)
print("All 3 business problems now have analytics-ready views.")
print("=" * 70)
