# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4a: AI/BI Genie Space Setup
# MAGIC
# MAGIC **Purpose**: Configure Databricks AI/BI Genie for natural language querying of Gold layer tables.
# MAGIC Enables business users to ask questions without writing SQL.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.1 Register Table & Column Descriptions for Genie
# MAGIC
# MAGIC Genie uses table and column comments to understand the data semantics.
# MAGIC The richer the descriptions, the more accurate NL-to-SQL translation becomes.

# COMMAND ----------

#  dim_customers 
spark.sql("""
ALTER TABLE gold.dim_customers
SET TBLPROPERTIES (
    'comment' = 'Unified customer master table. Contains deduplicated customer records across all 5 US regions (East, West, Central, South, North). Each customer_id is unique. Source: 6 regional customer systems.'
)
""")

column_comments_customers = {
    "customer_id": "Unique customer identifier (e.g., AB-10060). Primary key.",
    "customer_email": "Customer email address. May be NULL for North region customers.",
    "customer_name": "Full customer name in Title Case.",
    "segment": "Customer segment: Consumer, Corporate, or Home Office.",
    "country": "Customer country (currently all United States).",
    "city": "Customer city.",
    "state": "Customer US state.",
    "postal_code": "Customer postal/ZIP code.",
    "region": "GlobalMart region: East, West, Central, South, or North.",
}

for col, comment in column_comments_customers.items():
    spark.sql(f"ALTER TABLE gold.dim_customers ALTER COLUMN {col} COMMENT '{comment}'")

print("gold.dim_customers: table and column descriptions set")

# COMMAND ----------

#  dim_products 
spark.sql("""
ALTER TABLE gold.dim_products
SET TBLPROPERTIES (
    'comment' = 'Product catalog dimension. Contains 1,774 unique products with brand, category, color, and size information. Product IDs have category prefixes: FUR (Furniture), OFF (Office), TEC (Technology).'
)
""")

column_comments_products = {
    "product_id": "Unique product identifier with category prefix (e.g., FUR-CH-10001234).",
    "product_name": "Full product name/description.",
    "brand": "Product brand name.",
    "categories": "Comma-separated category hierarchy.",
    "primary_category": "Top-level category extracted from categories field.",
    "product_category_prefix": "Category prefix from product_id: FUR, OFF, or TEC.",
    "colors": "Available product colors.",
    "sizes": "Available product sizes.",
}

for col, comment in column_comments_products.items():
    spark.sql(f"ALTER TABLE gold.dim_products ALTER COLUMN {col} COMMENT '{comment}'")

print("gold.dim_products: table and column descriptions set")

# COMMAND ----------

#  dim_vendors 
spark.sql("""
ALTER TABLE gold.dim_vendors
SET TBLPROPERTIES (
    'comment' = 'Vendor/supplier reference table. Contains 7 vendors that supply products to GlobalMart.'
)
""")

spark.sql("ALTER TABLE gold.dim_vendors ALTER COLUMN vendor_id COMMENT 'Unique vendor identifier (e.g., VEN01).'")
spark.sql("ALTER TABLE gold.dim_vendors ALTER COLUMN vendor_name COMMENT 'Vendor company name.'")

print("gold.dim_vendors: table and column descriptions set")

# COMMAND ----------

#  fact_orders 
spark.sql("""
ALTER TABLE gold.fact_orders
SET TBLPROPERTIES (
    'comment' = 'Order-level fact table. Each row represents one product line within an order. Contains sales revenue, quantity, discount, profit, and delivery metrics. Join to dim_customers, dim_products, dim_vendors, and dim_date for analysis.'
)
""")

column_comments_orders = {
    "order_id": "Order identifier (e.g., CA-2017-138422). An order can have multiple product lines.",
    "customer_id": "FK to dim_customers. The customer who placed the order.",
    "product_id": "FK to dim_products. The product purchased.",
    "vendor_id": "FK to dim_vendors. The vendor/supplier.",
    "sales": "Gross sales amount in USD before discount.",
    "quantity": "Number of units purchased.",
    "discount": "Discount rate applied (0.0 to 1.0, where 0.2 = 20% off).",
    "profit": "Profit in USD. May be negative for loss-making sales. NULL for some Central region transactions.",
    "net_sales": "Sales after discount: sales × (1 - discount).",
    "delivery_days": "Days from purchase to customer delivery.",
    "delivery_variance_days": "Difference between estimated and actual delivery. Positive = early, negative = late.",
    "ship_mode": "Shipping method: First Class, Second Class, Standard Class, Same Day, or Unknown.",
    "payment_type": "Payment method used (e.g., credit_card).",
}

for col, comment in column_comments_orders.items():
    spark.sql(f"ALTER TABLE gold.fact_orders ALTER COLUMN {col} COMMENT '{comment}'")

print("gold.fact_orders: table and column descriptions set")

# COMMAND ----------

#  fact_returns 
spark.sql("""
ALTER TABLE gold.fact_returns
SET TBLPROPERTIES (
    'comment' = 'Returns fact table. Each row is one return event. Contains refund amount, reason, status, and customer context. Critical for fraud detection — use vw_customer_return_profile for aggregated fraud risk scoring.'
)
""")

column_comments_returns = {
    "order_id": "The order being returned. FK to fact_orders.",
    "customer_id": "FK to dim_customers. The customer making the return.",
    "return_reason": "Reason for return: Not Satisfied, Wrong Delivery, Defective Product, Late Delivery, Other.",
    "refund_amount": "Refund amount in USD.",
    "return_status": "Return status: Approved, Rejected, or Pending.",
    "return_date": "Date the return was initiated.",
    "days_to_return": "Number of days between order purchase and return initiation.",
}

for col, comment in column_comments_returns.items():
    spark.sql(f"ALTER TABLE gold.fact_returns ALTER COLUMN {col} COMMENT '{comment}'")

print("gold.fact_returns: table and column descriptions set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.2 Create Genie Space (Manual Step)
# MAGIC
# MAGIC After running this notebook, create a Genie Space in the Databricks UI:
# MAGIC
# MAGIC 1. Navigate to **AI/BI** → **Genie Spaces** → **Create Genie Space**
# MAGIC 2. Name: `GlobalMart POC`
# MAGIC 3. SQL Warehouse: Select your SQL Warehouse
# MAGIC 4. Add Tables:
# MAGIC    - `globalmart_poc.gold.dim_customers`
# MAGIC    - `globalmart_poc.gold.dim_products`
# MAGIC    - `globalmart_poc.gold.dim_vendors`
# MAGIC    - `globalmart_poc.gold.dim_date`
# MAGIC    - `globalmart_poc.gold.fact_orders`
# MAGIC    - `globalmart_poc.gold.fact_returns`
# MAGIC    - `globalmart_poc.gold.vw_revenue_reconciliation`
# MAGIC    - `globalmart_poc.gold.vw_customer_return_profile`
# MAGIC    - `globalmart_poc.gold.vw_inventory_performance`
# MAGIC 5. **General Instructions** (paste into Genie Space settings):

# COMMAND ----------

# Print Genie Space general instructions for copy-paste
genie_instructions = """
=== COPY THE BELOW INTO GENIE SPACE GENERAL INSTRUCTIONS ===

## GlobalMart POC — Genie Space Instructions

You are answering questions about GlobalMart, a US retail chain operating in 5 regions: East, West, Central, South, and North.

### Key Business Context:
- **Revenue questions**: Use `vw_revenue_reconciliation` for revenue by region/quarter/customer. The `potential_duplicate_flag` column flags orders that might be counted twice.
- **Returns & fraud questions**: Use `vw_customer_return_profile` for per-customer return summaries. `fraud_risk_score` ranges from 1 (low) to 5 (high risk). Use `fact_returns` for individual return records.
- **Inventory & product questions**: Use `vw_inventory_performance` for product sales velocity by region. `inventory_blindspot_flag` identifies products not selling in a region where they could be.
- **Profit**: Some transactions from the Central region have NULL profit values — do not assume 0.
- **Discount**: Stored as a decimal (0.2 = 20% discount), not a percentage.
- **Net sales**: Already calculated as `sales × (1 - discount)`.

### Sample Questions to Test:
1. "What is the total revenue by region?"
2. "Which customers have the highest fraud risk score?"
3. "Show me products that are selling well in one region but not selling in others"
4. "What is the quarterly revenue trend?"
5. "How many returns were approved vs rejected?"
6. "Which vendor has the most associated returns?"
7. "What are the top 10 customers by total spend?"

=== END OF GENIE INSTRUCTIONS ===
"""
print(genie_instructions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10.3 Verify Table Descriptions Are Set

# COMMAND ----------

# Verify all tables have descriptions set
gold_tables = [
    "gold.dim_customers", "gold.dim_products", "gold.dim_vendors",
    "gold.dim_date", "gold.dim_geography",
    "gold.fact_orders", "gold.fact_returns",
]

print("=" * 70)
print("GENIE READINESS — TABLE DESCRIPTIONS")
print("=" * 70)

for table in gold_tables:
    try:
        desc = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").filter(
            F.col("col_name") == "Comment"
        ).collect()
        comment = desc[0]["data_type"] if desc else "NO DESCRIPTION"
        status = "" if comment and comment != "NO DESCRIPTION" else ""
        print(f"  {status} {table}: {comment[:80]}...")
    except Exception as e:
        print(f"   {table}: {e}")

print("=" * 70)
