# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4d: AI-Powered Returns Fraud Investigator
# MAGIC
# MAGIC **Purpose**: Score customers based on 5 weighted return fraud rules. If their score
# MAGIC exceeds the threshold (50), use a Foundation Model to generate an actionable 
# MAGIC investigation brief for the Loss Prevention team.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import mlflow.deployments

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# 1. Calculate Customer-Level Return Metrics
returns_df = spark.sql("""
    SELECT 
        r.customer_id,
        MAX(c.customer_name) AS customer_name,
        COUNT(r.order_id) AS total_returns,
        ROUND(SUM(r.refund_amount), 2) AS total_return_value,
        COUNT(DISTINCT c.region) AS regions_returned_in,
        MAX(CASE WHEN r.order_purchase_date IS NULL THEN 1 ELSE 0 END) AS has_orphan_return,
        AVG(r.days_to_return) AS avg_days_to_return
    FROM gold.fact_returns r
    LEFT JOIN gold.dim_customers c ON r.customer_id = c.customer_id
    GROUP BY r.customer_id
""")

# 2. Apply the 5-Rule Weighted Scoring System
# Max Score = 100
# Threshold = 50

scored_df = returns_df.withColumn(
    "score_no_matching_order", F.when(F.col("has_orphan_return") == 1, 40).otherwise(0)
).withColumn(
    "score_high_volume", F.when(F.col("total_returns") > 3, 20).otherwise(0)
).withColumn(
    "score_high_value", F.when(F.col("total_return_value") > 500, 20).otherwise(0)
).withColumn(
    "score_policy_expired", F.when(F.col("avg_days_to_return") > 30, 10).otherwise(0)
).withColumn(
    "score_multi_region", F.when(F.col("regions_returned_in") > 1, 10).otherwise(0)
)

scored_df = scored_df.withColumn(
    "anomaly_score",
    F.col("score_no_matching_order") + F.col("score_high_volume") + 
    F.col("score_high_value") + F.col("score_policy_expired") + F.col("score_multi_region")
).withColumn(
    "violated_rules",
    F.concat_ws(", ",
        F.when(F.col("score_no_matching_order") > 0, F.lit("No Matching Order")),
        F.when(F.col("score_high_volume") > 0, F.lit("High Return Volume (>3)")),
        F.when(F.col("score_high_value") > 0, F.lit("High Refund Value (>$500)")),
        F.when(F.col("score_policy_expired") > 0, F.lit("Policy Expired (>30 Days)")),
        F.when(F.col("score_multi_region") > 0, F.lit("Cross-Region Returns"))
    )
)

# 3. Filter for Flagged Customers (Score >= 50)
flagged_customers = scored_df.filter(F.col("anomaly_score") >= 50).collect()

print(f"Flagged {len(flagged_customers)} customers for investigation.")

# COMMAND ----------

# 4. Generate AI Investigation Briefs
def generate_brief(name, returns, value, rules, score):
    prompt = f"""You are a Loss Prevention AI assisting the GlobalMart Returns Team.
Customer '{name}' was flagged with an Anomaly Score of {score}/100 based on these metrics:
- Total Returns: {returns}
- Total Refund Value: ${value}
- Violated Rules: {rules}

Write an investigation brief (3 paragraphs maximum) addressing:
1. What specific patterns make this case suspicious based on the exact data points above.
2. What innocent explanations could exist alongside these potential fraud indicators.
3. What the returns team should verify first (specific, actionable steps).

Be concise and professional.
"""
    try:
        client = mlflow.deployments.get_deploy_client("databricks")
        resp = client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": [{"role": "system", "content": "You are a Loss Prevention investigator."},
                             {"role": "user", "content": prompt}],
                "max_tokens": 250,
                "temperature": 0.2
            }
        )
        return resp["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"Brief generation failed: {e}"

results = []
for row in flagged_customers:
    brief = generate_brief(
        row["customer_name"], row["total_returns"], row["total_return_value"], 
        row["violated_rules"], row["anomaly_score"]
    )
    result_dict = row.asDict()
    result_dict["ai_investigation_brief"] = brief
    results.append(result_dict)

# COMMAND ----------

# 5. Write to Gold Layer
if results:
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_returns", IntegerType(), True),
        StructField("total_return_value", DoubleType(), True),
        StructField("regions_returned_in", IntegerType(), True),
        StructField("has_orphan_return", IntegerType(), True),
        StructField("avg_days_to_return", DoubleType(), True),
        StructField("score_no_matching_order", IntegerType(), True),
        StructField("score_high_volume", IntegerType(), True),
        StructField("score_high_value", IntegerType(), True),
        StructField("score_policy_expired", IntegerType(), True),
        StructField("score_multi_region", IntegerType(), True),
        StructField("anomaly_score", IntegerType(), True),
        StructField("violated_rules", StringType(), True),
        StructField("ai_investigation_brief", StringType(), True),
    ])
    
    final_df = spark.createDataFrame(results, schema).withColumn("generation_timestamp", F.current_timestamp())
    final_df.write.mode("overwrite").saveAsTable("gold.flagged_return_customers")
    print(f"gold.flagged_return_customers successfully written with {len(results)} briefs.")
else:
    print("No customers met the threshold for investigation.")
