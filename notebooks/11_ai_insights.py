# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4b: AI-Powered Data Quality Narratives & Insights
# MAGIC
# MAGIC **Purpose**: Use Databricks Foundation Model endpoints to generate:
# MAGIC 1. Plain-English data quality reports from quarantine tables
# MAGIC 2. Fraud risk narratives for leadership
# MAGIC 3. Inventory redistribution recommendations
# MAGIC 4. Revenue anomaly explanations

# COMMAND ----------

from pyspark.sql import functions as F
import json

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.1 Gather Data Quality Statistics

# COMMAND ----------

def get_quarantine_summary():
    """Collect DQ quarantine statistics into a structured summary."""
    summary = {}

    quarantine_tables = {
        "silver.customers_quarantine": "Customers",
        "silver.orders_quarantine": "Orders",
        "silver.transactions_quarantine": "Transactions",
        "silver.returns_quarantine": "Returns",
    }

    for table, label in quarantine_tables.items():
        try:
            df = spark.table(table)
            count = df.count()

            if count > 0:
                # Get failure reason breakdown
                reasons = (
                    df.select("_dq_failure_reasons")
                    .groupBy("_dq_failure_reasons")
                    .count()
                    .orderBy(F.desc("count"))
                    .collect()
                )
                reason_breakdown = [
                    {"reason": row["_dq_failure_reasons"], "count": row["count"]}
                    for row in reasons
                ]
            else:
                reason_breakdown = []

            summary[label] = {
                "quarantined_count": count,
                "reasons": reason_breakdown
            }
        except Exception:
            summary[label] = {"quarantined_count": 0, "reasons": [], "note": "No quarantine table found"}

    return summary


def get_pipeline_stats():
    """Collect overall pipeline statistics."""
    stats = {}

    tables = {
        "silver.customers": "Customers",
        "silver.orders": "Orders",
        "silver.transactions": "Transactions",
        "silver.returns": "Returns",
        "silver.products": "Products",
        "silver.vendors": "Vendors",
    }

    for table, label in tables.items():
        try:
            stats[label] = spark.table(table).count()
        except Exception:
            stats[label] = 0

    return stats


dq_summary = get_quarantine_summary()
pipeline_stats = get_pipeline_stats()

print("DQ Summary:")
print(json.dumps(dq_summary, indent=2))
print("\nPipeline Stats:")
print(json.dumps(pipeline_stats, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.2 Generate AI-Powered Data Quality Narrative
# MAGIC
# MAGIC Uses Databricks Foundation Model serving endpoint to produce executive-friendly summaries.

# COMMAND ----------

import mlflow.deployments

def generate_dq_narrative(dq_summary: dict, pipeline_stats: dict) -> str:
    """Use a Foundation Model to generate a data quality narrative."""

    prompt = f"""You are a data quality analyst at GlobalMart, a US retail chain.
Write a clear, executive-friendly data quality summary report based on the following statistics.

## Pipeline Statistics (Clean Records in Silver Layer):
{json.dumps(pipeline_stats, indent=2)}

## Data Quality Quarantine Summary:
{json.dumps(dq_summary, indent=2)}

## Instructions:
- Write 3-5 paragraphs in plain English suitable for a board presentation
- Start with an overall health assessment (what % of data passed quality checks)
- Highlight the most critical quality issues and their potential business impact
- Mention specific numbers (records quarantined, types of failures)
- End with recommendations for improving data quality at the source
- Use a professional but accessible tone — avoid technical jargon
- DO NOT use bullet points — write in narrative paragraph form
"""

    # Call Databricks Foundation Model endpoint
    client = mlflow.deployments.get_deploy_client("databricks")

    response = client.predict(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        inputs={
            "messages": [
                {"role": "system", "content": "You are a senior data quality analyst writing reports for executive leadership."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 1000,
            "temperature": 0.3,
        }
    )

    return response["choices"][0]["message"]["content"]


# Generate the narrative
try:
    dq_narrative = generate_dq_narrative(dq_summary, pipeline_stats)
    print("=" * 70)
    print("AI-GENERATED DATA QUALITY REPORT")
    print("=" * 70)
    print(dq_narrative)
    print("=" * 70)
except Exception as e:
    print(f"Could not generate AI narrative: {e}")
    print("Ensure a Foundation Model endpoint is available in your workspace.")
    dq_narrative = "AI narrative generation requires a Foundation Model endpoint."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.3 Fraud Risk Intelligence Narrative

# COMMAND ----------

def get_fraud_summary():
    """Collect fraud risk statistics."""
    fraud_stats = spark.sql("""
        SELECT
            fraud_risk_label,
            COUNT(*) AS customer_count,
            ROUND(SUM(total_refund_amount), 2) AS total_refunds,
            ROUND(AVG(total_returns), 1) AS avg_returns_per_customer,
            MAX(total_returns) AS max_returns
        FROM gold.vw_customer_return_profile
        GROUP BY fraud_risk_label
        ORDER BY customer_count DESC
    """).collect()

    high_risk = spark.sql("""
        SELECT customer_id, customer_name, home_region, total_returns,
               total_refund_amount, fraud_risk_score, fraud_risk_label
        FROM gold.vw_customer_return_profile
        WHERE fraud_risk_score >= 4
        ORDER BY total_refund_amount DESC
        LIMIT 10
    """).collect()

    return {
        "risk_distribution": [row.asDict() for row in fraud_stats],
        "top_high_risk_customers": [row.asDict() for row in high_risk]
    }

fraud_data = get_fraud_summary()

def generate_fraud_narrative(fraud_data: dict) -> str:
    """Generate a fraud risk intelligence narrative."""
    prompt = f"""You are a fraud risk analyst at GlobalMart, a US retail chain with 5 regions.
Write an executive intelligence brief about return fraud risk based on the following data.

## Fraud Risk Distribution:
{json.dumps(fraud_data['risk_distribution'], indent=2, default=str)}

## Top High-Risk Customers:
{json.dumps(fraud_data['top_high_risk_customers'], indent=2, default=str)}

## Instructions:
- Write 3-4 paragraphs summarizing the fraud risk landscape
- Quantify the financial exposure from high-risk customers
- Recommend specific actions (e.g., which customers need immediate review)
- Note that before this unified platform, these patterns were invisible because return data was fragmented across 6 systems
- Professional tone suitable for the VP of Loss Prevention
"""

    client = mlflow.deployments.get_deploy_client("databricks")
    response = client.predict(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        inputs={
            "messages": [
                {"role": "system", "content": "You are a senior fraud risk analyst writing intelligence briefs for executive leadership."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 800,
            "temperature": 0.3,
        }
    )
    return response["choices"][0]["message"]["content"]

try:
    fraud_narrative = generate_fraud_narrative(fraud_data)
    print("=" * 70)
    print("AI-GENERATED FRAUD RISK INTELLIGENCE BRIEF")
    print("=" * 70)
    print(fraud_narrative)
    print("=" * 70)
except Exception as e:
    print(f"Could not generate fraud narrative: {e}")
    fraud_narrative = "Requires Foundation Model endpoint."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.4 Inventory Intelligence Narrative

# COMMAND ----------

def get_inventory_summary():
    """Collect inventory performance statistics."""
    velocity = spark.sql("""
        SELECT region, velocity_category, COUNT(*) AS product_count,
               ROUND(SUM(COALESCE(total_revenue, 0)), 2) AS revenue
        FROM gold.vw_inventory_performance
        GROUP BY region, velocity_category
        ORDER BY region, revenue DESC
    """).collect()

    blindspots = spark.sql("""
        SELECT product_id, product_name, region,
               total_units_sold, total_revenue, selling_regions
        FROM gold.vw_inventory_performance
        WHERE velocity_category = 'Slow Mover'
        ORDER BY total_revenue ASC
        LIMIT 10
    """).collect()

    return {
        "velocity_by_region": [row.asDict() for row in velocity],
        "slowest_movers": [row.asDict() for row in blindspots]
    }

inventory_data = get_inventory_summary()

def generate_inventory_narrative(inv_data: dict) -> str:
    """Generate inventory intelligence narrative."""
    prompt = f"""You are a merchandising analyst at GlobalMart, a US retail chain with 5 regions.
Write an executive brief about inventory performance and redistribution opportunities.

## Sales Velocity by Region:
{json.dumps(inv_data['velocity_by_region'], indent=2, default=str)}

## Slowest-Moving Products:
{json.dumps(inv_data['slowest_movers'], indent=2, default=str)}

## Instructions:
- Write 3-4 paragraphs about inventory performance insights
- Identify regions with slow-moving inventory that could be redistributed
- Quantify the revenue opportunity from redistribution
- Note that before this platform, regional managers had no visibility into other regions' inventory
- Recommend specific actions for the merchandising team
- Professional tone suitable for the VP of Merchandising
"""

    client = mlflow.deployments.get_deploy_client("databricks")
    response = client.predict(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        inputs={
            "messages": [
                {"role": "system", "content": "You are a senior merchandising analyst writing intelligence briefs for executive leadership."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 800,
            "temperature": 0.3,
        }
    )
    return response["choices"][0]["message"]["content"]

try:
    inv_narrative = generate_inventory_narrative(inventory_data)
    print("=" * 70)
    print("AI-GENERATED INVENTORY INTELLIGENCE BRIEF")
    print("=" * 70)
    print(inv_narrative)
    print("=" * 70)
except Exception as e:
    print(f"Could not generate inventory narrative: {e}")
    inv_narrative = "Requires Foundation Model endpoint."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11.5 Save AI Narratives as a Summary Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

narratives_data = [
    ("data_quality_report", dq_narrative if 'dq_narrative' in dir() else ""),
    ("fraud_risk_brief", fraud_narrative if 'fraud_narrative' in dir() else ""),
    ("inventory_intelligence", inv_narrative if 'inv_narrative' in dir() else ""),
]

narratives_df = (
    spark.createDataFrame(narratives_data, ["report_type", "narrative"])
    .withColumn("generated_at", F.current_timestamp())
    .withColumn("model_used", F.lit("databricks-meta-llama-3-3-70b-instruct"))
)

narratives_df.write.mode("overwrite").saveAsTable("gold.ai_narratives")
print(f"gold.ai_narratives: {spark.table('gold.ai_narratives').count()} reports saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The AI Intelligence Layer is now configured:
# MAGIC
# MAGIC | Component | Status |
# MAGIC |-----------|--------|
# MAGIC | Table/Column descriptions for Genie | Set on all Gold tables |
# MAGIC | Genie Space instructions | Generated (manual Genie Space creation required) |
# MAGIC | DQ Narrative | Generated via Foundation Model |
# MAGIC | Fraud Risk Brief | Generated via Foundation Model |
# MAGIC | Inventory Intelligence | Generated via Foundation Model |
# MAGIC | Narratives stored | `gold.ai_narratives` table |
# MAGIC
# MAGIC **Next**: Create the Genie Space in the Databricks UI and test natural language queries.
