# GlobalMart — GenAI Track: Final Assessment

## Q1. Prompt Design — Two Use Cases Compared

**UC1 (DQ Audit Reporter) vs UC4 (Business Insights)**

For UC1, the prompt needed to be tightly constrained. We were asking the model to translate a specific database failure — a count, an entity name, and an issue type — into accounting language. The risk of a vague output was high if we left the structure open-ended, so we passed exactly three variables and explicitly listed three questions the model had to answer, in order, without bullet points. We knew it was working when the outputs consistently cited the exact count we provided and named real business reports (like "Regional Profitability by Segment") rather than generic phrases.

For UC4, the prompt was looser by design. We passed a JSON block of aggregated KPIs and asked the model to behave like a VP of Analytics finding the outlier in the data. There was no benefit to prescribing the three-step structure here — the model needed flexibility to navigate the JSON and identify which region or vendor had the highest or lowest value. We confirmed it was working when the generated paragraphs called out specific numbers from the KPI data (e.g., a specific region's revenue figure) without us telling it which one was the outlier.

The core difference: UC1 needed structure because the output format was fixed; UC4 needed freedom because the analytical reasoning was the output.

---

## Q2. Why Is a Response Parser Needed?

The LLM endpoint returns a deeply nested API response object — not just the text. A typical MLflow `client.predict()` response looks like:

```python
response["choices"][0]["message"]["content"]
```

Alongside the text, the API includes token counts, the finish reason, model metadata, and other fields. If we wrote the raw response object directly into our Gold tables (`gold.dq_audit_report`, `gold.ai_business_insights`), those columns would contain multi-kilobyte JSON dictionaries instead of readable paragraphs.

For the DQ audit report, this would mean Finance auditors reading raw JSON rather than plain English. For the business insights table, dashboards built on top of it would fail to display the content correctly. The parser strips everything away and gives us just the text string.

---

## Q3. Rules for Detection, LLM for Explanation

Using an LLM to decide whether a return is suspicious is a bad idea for several reasons.

First, it's slow. Calling the model row-by-row across thousands of transactions would bottleneck the pipeline. Second, it's expensive — token costs add up quickly at that scale. Third, and most importantly, it's not auditable. If the fraud team needs to defend a decision to block a customer's refund, "the model thought it looked suspicious" is not a defensible explanation. A rule like "5 returns in 90 days with no matching purchase order = score of 60" is reproducible, explainable, and contestable.

The LLM's role in UC2 is to take those rule-based scores and translate them into a coherent, human-readable investigation brief. It doesn't decide anything — it explains. That's the right use of a language model in a classification-adjacent workflow.

---

## Q4. Which Use Case Would Need the Most Redesign for Production?

UC3 — the RAG Product Q&A system — would need the most significant rework.

The current implementation loads the entire Gold dimension table into a Pandas dataframe on the cluster driver, generates embeddings in memory with `sentence-transformers`, and builds a FAISS index that lives only for the duration of the notebook run. That works fine as a prototype, but it doesn't survive incremental data updates. Every time a new product is added or an inventory metric changes, the entire index has to be rebuilt from scratch.

In production, this would need to be replaced with Databricks Vector Search — a managed vector store that syncs automatically with Delta tables. Embeddings would be generated as part of the pipeline when new rows are written, and the index would stay current without any manual rebuilding. The query logic would also need to move from a local FAISS call to a Vector Search endpoint, which scales separately from the compute cluster.
