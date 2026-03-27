# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3b: Gold Layer — Fact Tables
# MAGIC
# MAGIC **Purpose**: Build order and return fact tables by joining Silver layer entities.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE CATALOG globalmart_poc")

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
print(f"gold.fact_orders: {count} rows")

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
print(f"gold.fact_returns: {count} rows")

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
    print(f"  {tbl}: {count} rows, {cols} columns")
print("=" * 60)
