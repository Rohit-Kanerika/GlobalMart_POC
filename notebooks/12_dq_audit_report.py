# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4c: AI-Powered DQ Audit Reporter
# MAGIC
# MAGIC **Purpose**: Generate a row-level plain-English business impact explanation for each
# MAGIC unique data quality issue found in the Silver quarantine tables using Foundation Models.
# MAGIC Output feeds directly into the `gold.dq_audit_report` table for the Finance Audit team.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import mlflow.deployments

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# 1. Gather all quarantine tables and group by issue type
quarantine_tables = {
    "silver.customers_quarantine": "customers",
    "silver.orders_quarantine": "orders",
    "silver.transactions_quarantine": "transactions",
    "silver.returns_quarantine": "returns",
}

audit_records = []

for table_name, entity in quarantine_tables.items():
    try:
        # Group by the specific failure reason
        issues_df = spark.sql(f"""
            SELECT _dq_failure_reasons as issue_type, COUNT(*) as rejected_count
            FROM {table_name}
            GROUP BY _dq_failure_reasons
        """).collect()
        
        for row in issues_df:
            audit_records.append({
                "entity_name": entity,
                "issue_type": row["issue_type"],
                "rejected_count": row["rejected_count"]
            })
    except Exception as e:
        print(f"Skipping {table_name}: {e}")

# COMMAND ----------

# 2. Define the LLM Prompt Template and Generation Function

def generate_business_impact(entity: str, issue_type: str, count: int) -> str:
    prompt = f"""You are a Data Governance AI assisting the GlobalMart Finance team with an external audit.
We rejected {count} records from the '{entity}' pipeline due to the following data quality issue:
"{issue_type}"

Write a plain-English explanation (3-4 sentences maximum) addressing:
1. What the problem is and what pattern triggered the rejection.
2. Why this specific record type cannot be accepted into the analytics layer.
3. What specific business report, audit figure, or operational decision is at risk if this data were included.

Do not use bullet points. State the facts clearly and professionally.
"""
    try:
        client = mlflow.deployments.get_deploy_client("databricks")
        response = client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": [
                    {"role": "system", "content": "You are a senior financial auditor and data governance analyst."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 150,
                "temperature": 0.2,
            }
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"Explanation generation failed: {e}"

# COMMAND ----------

# 3. Generate explanations for each issue group
print(f"Processing {len(audit_records)} unique DQ issue groups...")

for record in audit_records:
    print(f"Generating for {record['entity_name']} -> {record['issue_type']} ({record['rejected_count']} rows)...")
    record["ai_business_impact_explanation"] = generate_business_impact(
        record["entity_name"], 
        record["issue_type"], 
        record["rejected_count"]
    )

# COMMAND ----------

# 4. Write out to gold.dq_audit_report

schema = StructType([
    StructField("entity_name", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("rejected_count", IntegerType(), True),
    StructField("ai_business_impact_explanation", StringType(), True)
])

# Create dataframe from records
audit_df = spark.createDataFrame(audit_records, schema)

# Add timestamp
final_df = audit_df.withColumn("generation_timestamp", F.current_timestamp())

# Write to Gold
final_df.write.mode("overwrite").saveAsTable("gold.dq_audit_report")

print(f"gold.dq_audit_report successfully written with {final_df.count()} issue groups explained.")
display(final_df)
