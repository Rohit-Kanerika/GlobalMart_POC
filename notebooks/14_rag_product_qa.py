# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4e: AI-Powered RAG Product Q&A
# MAGIC
# MAGIC **Purpose**: Convert enriched structured product/vendor data into an unstructured vector index 
# MAGIC using FAISS and local embeddings to answer natural language semantic queries, logging all Q&A history.

# COMMAND ----------

# MAGIC %pip install "numpy<2.0.0" sentence-transformers faiss-cpu langchain langchain-community

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import mlflow.deployments
import pandas as pd

spark.sql("USE CATALOG globalmart_poc")

# COMMAND ----------

# 1. Enrich dim_products with Vendor and Performance Metrics
# We join the Gold dimension tables with the Analytics Views to get a rich dataset.

product_docs_df = spark.sql("""
    SELECT 
        p.product_id, 
        p.product_name, 
        p.brand, 
        p.primary_category,
        v.vendor_name,
        COALESCE(i.velocity_category, 'No Sales') AS velocity_category,
        COALESCE(i.total_revenue, 0) AS total_revenue,
        COALESCE(i.regions_with_sales, 0) AS regions_active
    FROM gold.dim_products p
    LEFT JOIN gold.fact_orders o ON p.product_id = o.product_id
    LEFT JOIN gold.dim_vendors v ON o.vendor_id = v.vendor_id
    LEFT JOIN gold.vw_inventory_performance i ON p.product_id = i.product_id
    GROUP BY p.product_id, p.product_name, p.brand, p.primary_category, v.vendor_name, i.velocity_category, i.total_revenue, i.regions_with_sales
""")

# Convert Spark DF to Pandas for local embedding
pdf = product_docs_df.toPandas()

# COMMAND ----------

# 2. Convert Structured Rows into Natural Language Documents
# RAG models embed semantic text, not database rows.

def row_to_text(row):
    return (
        f"Product Name: {row['product_name']}. "
        f"Brand: {row['brand']}. Category: {row['primary_category']}. "
        f"Supplied by Vendor: {row['vendor_name']}. "
        f"Performance: This product is considered a '{row['velocity_category']}' "
        f"with ${row['total_revenue']} in revenue across {row['regions_active']} regions."
    )

pdf['document_text'] = pdf.apply(row_to_text, axis=1)

print("Sample Document String:")
print(pdf['document_text'].iloc[0])

# COMMAND ----------

# 3. Local Embedding and FAISS Index Building

print("Loading local embedding model: all-MiniLM-L6-v2...")
embedder = SentenceTransformer('all-MiniLM-L6-v2')

documents = pdf['document_text'].tolist()
print(f"Embedding {len(documents)} documents...")

# Generate document embeddings
doc_embeddings = embedder.encode(documents, convert_to_numpy=True)
dimension = doc_embeddings.shape[1]

# Build FAISS Index
index = faiss.IndexFlatL2(dimension)
index.add(doc_embeddings)
print(f"FAISS index built with {index.ntotal} documents of dimension {dimension}.")

# COMMAND ----------

# 4. RAG Retrieval and LLM Generation

def retrieve_documents(query, k=5):
    query_vector = embedder.encode([query], convert_to_numpy=True)
    distances, indices = index.search(query_vector, k)
    retrieved = [documents[i] for i in indices[0]]
    return retrieved

def ask_product_rag(question):
    # Retrieve top 5 contexts
    context_docs = retrieve_documents(question, k=5)
    context_str = "\n".join([f"- {doc}" for doc in context_docs])
    
    prompt = f"""You are a helpful GlobalMart merchandising assistant.
Answer the user's question using ONLY the provided context documents.
If the answer is not contained in the context documents, reply exactly with: 
"I cannot answer this question based on the retrieved documents."

Context Documents:
{context_str}

Question: {question}
Answer:"""

    try:
        client = mlflow.deployments.get_deploy_client("databricks")
        response = client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": [
                    {"role": "system", "content": "You are a precise and helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 150,
                "temperature": 0.1,
            }
        )
        answer = response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        answer = f"Error generating answer: {e}"
        
    return answer, context_str

# COMMAND ----------

# 5. Run 5 Test Questions & Log to Gold History Table

test_questions = [
    "Which products are classified as slow-moving?",
    "Which vendor supplies the Home Office chair?",
    "Are there any products with over $5000 in revenue?",
    "Which regions have active sales for the Samsung Monitor?",
    "What is the capital of France?" # Should fail/trigger fall-back
]

history_records = []

for q in test_questions:
    print(f"\nQ: {q}")
    ans, docs = ask_product_rag(q)
    print(f"A: {ans}")
    
    history_records.append({
        "question": q,
        "answer": ans,
        "retrieved_documents": docs
    })

# Write to globalmart.gold.rag_query_history
schema = StructType([
    StructField("question", StringType(), True),
    StructField("answer", StringType(), True),
    StructField("retrieved_documents", StringType(), True),
])

history_df = spark.createDataFrame(history_records, schema).withColumn("timestamp", F.current_timestamp())
history_df.write.mode("append").saveAsTable("gold.rag_query_history")
print("\nQueries logged to globalmart.gold.rag_query_history")
