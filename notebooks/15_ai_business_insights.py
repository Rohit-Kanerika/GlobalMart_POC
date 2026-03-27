# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4f: AI Business Insights & Native LLM Functions
# MAGIC
# MAGIC **Purpose**: Generate executive-level narrative summaries from structured KPI aggregations
# MAGIC across 3 business domains. Additionally, demonstrate native SQL `ai_query()` usage.
# MAGIC Output feeds directly into `gold.ai_business_insights`.
# MAGIC
# MAGIC *Note: Never pass raw tables to an LLM. Always aggregate first to avoid token overruns.*

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import mlflow.deployments
import json

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# 1. Gather Aggregated KPIs (DO NOT PASS RAW ROWS)

# Domain 1: Revenue Performance by Region
val_rev = spark.sql("""
    SELECT region, ROUND(SUM(gross_revenue), 2) as regional_revenue, count(DISTINCT customer_id) as customers
    FROM gold.vw_revenue_reconciliation
    GROUP BY region
""").collect()
json_rev = json.dumps([row.asDict() for row in val_rev])

# Domain 2: Vendor Return Rates
val_vendor = spark.sql("""
    SELECT v.vendor_name, COUNT(r.order_id) as total_returns, ROUND(SUM(r.refund_amount), 2) as total_refunds
    FROM gold.fact_returns r
    JOIN gold.dim_vendors v ON r.vendor_id = v.vendor_id
    GROUP BY v.vendor_name
    ORDER BY total_refunds DESC
    LIMIT 5
""").collect()
json_vendor = json.dumps([row.asDict() for row in val_vendor])

# Domain 3: Slow-Moving Inventory
val_inv = spark.sql("""
    SELECT region, velocity_category, COUNT(product_id) as product_count, ROUND(SUM(total_revenue), 2) as tied_revenue
    FROM gold.vw_inventory_performance
    WHERE velocity_category = 'Slow Mover'
    GROUP BY region, velocity_category
    ORDER BY tied_revenue DESC
""").collect()
json_inv = json.dumps([row.asDict() for row in val_inv])

# COMMAND ----------

# 2. Executive Summary Generation via Foundation Model API

def generate_exec_summary(domain: str, json_data: str) -> str:
    prompt = f"""You are a VP of Analytics at GlobalMart.
Analyze the following aggregated KPI data for the '{domain}' domain:
{json_data}

Write an executive summary (4-6 sentences) explaining:
1. What the overall performance pattern reveals.
2. Which specific region or vendor is the largest outlier or risk.
3. A strategic recommendation for the executive team.

Do not use bullet points or print raw JSON. Write a fluent paragraph.
"""
    try:
        client = mlflow.deployments.get_deploy_client("databricks")
        resp = client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": [{"role": "system", "content": "You are a senior executive."},
                             {"role": "user", "content": prompt}],
                "max_tokens": 250,
                "temperature": 0.2
            }
        )
        return resp["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"Summary generation failed: {e}"

insights = [
    {"insight_type": "revenue_performance", "kpi_data_json": json_rev, "ai_generated_executive_summary": generate_exec_summary("Revenue Performance", json_rev)},
    {"insight_type": "vendor_return_rate", "kpi_data_json": json_vendor, "ai_generated_executive_summary": generate_exec_summary("Vendor Return Rates", json_vendor)},
    {"insight_type": "slow_moving_inventory", "kpi_data_json": json_inv, "ai_generated_executive_summary": generate_exec_summary("Slow-Moving Inventory", json_inv)}
]

# COMMAND ----------

# 3. Write output to gold.ai_business_insights

schema = StructType([
    StructField("insight_type", StringType(), True),
    StructField("kpi_data_json", StringType(), True),
    StructField("ai_generated_executive_summary", StringType(), True)
])

df_insights = spark.createDataFrame(insights, schema).withColumn("generation_timestamp", F.current_timestamp())
df_insights.write.mode("overwrite").saveAsTable("gold.ai_business_insights")

print("Executive Summaries written to gold.ai_business_insights")
display(df_insights)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Native SQL `ai_query()` Demonstrations
# MAGIC
# MAGIC Databricks permits calling LLMs directly within SQL to classify or evaluate existing rows!
# MAGIC *(Note: Requires a Serving Endpoint named `databricks-meta-llama-3-3-70b-instruct`)*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demo 1: Sentiment Classification on Product Names
# MAGIC -- Ask the LLM to classify if a product sounds "Premium", "Standard", or "Budget"
# MAGIC SELECT 
# MAGIC     product_name,
# MAGIC     brand,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT('Classify this product into one word strictly (Premium, Standard, or Budget) based on its name and brand: ', product_name, ' by ', brand)
# MAGIC     ) AS ai_perceived_tier
# MAGIC FROM gold.dim_products
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Demo 2: Generating Custom Email Subject Lines for High-Risk Fraud Customers
# MAGIC -- We use the LLM inline to craft a personalized warning subject line for fraudulent returners.
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     total_returns,
# MAGIC     total_return_value,
# MAGIC     ai_query(
# MAGIC         'databricks-meta-llama-3-3-70b-instruct',
# MAGIC         CONCAT('Write a stern but polite 5-word email subject line to customer ', customer_name, ' who has returned $', total_return_value, ' regarding their account status.')
# MAGIC     ) AS ai_email_subject
# MAGIC FROM gold.flagged_return_customers
# MAGIC LIMIT 5;
