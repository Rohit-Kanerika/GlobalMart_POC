# GlobalMart GenAI Track — Team Summary

## What We Built

The goal for the GenAI track was to turn the Gold layer from a queryable data store into a system that actively generates insight on its own. We built four notebooks, each targeting a different audience and a different kind of output.

**UC1 — DQ Audit Reporter**: This reads from the Silver quarantine tables, groups all rejected records by issue type, and uses the Foundation Model to write a short paragraph explaining each issue in finance-team language — what failed, why it can't be included, and what report is at risk. The output goes to `gold.dq_audit_report` as an auditor-ready table.

**UC2 — Returns Fraud Investigator**: This applies a deterministic five-rule scoring system across every customer's return history. Customers who exceed the 50-point threshold are flagged, and the model generates a case-specific investigation brief for each one, citing the actual data points that triggered the score. Results land in `gold.flagged_return_customers`.

**UC3 — RAG Product Q&A and Genie**: This converts the enriched product and vendor dimension data into natural language documents, embeds them locally using `sentence-transformers`, and builds a FAISS vector index. Business users can ask semantic questions about product performance and inventory. For analytical questions that need aggregation, we set up a Databricks Genie Space on the Gold layer tables. Results from the RAG system are logged to `gold.rag_query_history`.

**UC4 — Executive Business Insights**: This aggregates KPIs from the analytics views and sends those summaries to the model with instructions to generate a narrative paragraph per domain. Revenue performance, vendor return rates, and slow-moving inventory each get a four-to-six sentence executive summary written in plain English, stored in `gold.ai_business_insights`.

---

## What Surprised Us

The biggest surprise was how cleanly the boundary between RAG and Genie fell out in practice. Going in, the assumption was that RAG could probably handle most product questions. In reality, the moment we tested a question like "which vendor has the highest return rate?", the limitation became obvious — FAISS retrieves five text chunks by semantic similarity, and no amount of semantic matching can substitute for a `MAX GROUP BY` across the full dataset. Genie, which writes SQL against the actual tables, answered that question in seconds.

It turned out to be a useful lesson about where GenAI fits versus where structured computation fits. They complement each other rather than compete.

---

## What We Learned

The most durable pattern from this build: use rules to detect, use LLMs to explain. Every time we tried to offload a classification decision to the model, we ran into reproducibility and cost problems. Every time we used the model purely to narrate a decision already made by deterministic code, the output was better, faster, and more defensible.

The biggest challenge going forward would be productionising UC3. The local FAISS index doesn't survive incremental data updates — it has to be rebuilt in full every run. Moving to Databricks Vector Search with automatic sync to Delta tables is the right path, but it's a meaningful redesign of the retrieval layer.
