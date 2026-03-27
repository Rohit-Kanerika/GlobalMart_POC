# GlobalMart — UC3: RAG Product Q&A and Genie

## Implementation Notes

### Why does a structured Gold table need to be converted to text before embedding?

`dim_products` is a table of rows and columns — integers, strings, and foreign key references. Embedding models like `sentence-transformers` work by mapping the semantic meaning of natural language into dense numerical vectors. They cannot parse a SQL schema, and they have no concept of a foreign key relationship between `vendor_id` and `dim_vendors`.

The conversion step bridges this gap. For each product row, we join the dimension table with the vendor name and the performance metrics from `vw_inventory_performance`, then concatenate the combined result into a descriptive sentence:

> "Product Name: Samsung 27-inch Monitor. Brand: Samsung. Category: Electronics. Supplied by Vendor: TechGlobal Inc. Performance: This product is considered a 'Fast Mover' with $18,400 in revenue across 4 regions."

When this text is embedded, the model captures the relationships between the product, its supplier, its category, and its sales performance as semantic proximity in the vector space. A question about "fast-moving electronics" will geometrically land near the above document in the index.

---

### How did you decide what metadata to include in each product document?

The driving question was: what does a merchandiser actually need to know when asking a business question? They want to know what the product is, who supplies it, which category it belongs to, and whether it's selling. That's why we included `vendor_name`, `primary_category`, `velocity_category`, and `total_revenue` in every document.

Omitting the `velocity_category` would mean that a question like "which products are slow-moving?" would return documents with no semantic match. The concept of "slow moving" simply wouldn't exist in the embedding space for those products. The system would pull in contextually irrelevant documents and the LLM would correctly refuse to answer because nothing in the retrieved text addresses the question.

Similarly, omitting the vendor name would make it impossible to answer vendor-specific questions from RAG — the product document would contain no vendor signal at all.

---

### Testing "Which vendor has the highest return rate?"

When we tested this question, FAISS retrieved `k=5` documents — the five product entries whose text embeddings were geometrically closest to the encoded query. The system correctly identified products associated with the word "return" in their descriptions.

However, the system could not answer the question accurately. RAG retrieves a sample of five text chunks, not the full dataset. Finding the vendor with the *highest* return rate requires a `MAX(return_rate) GROUP BY vendor_name` computation across every vendor in the system — something fundamentally impossible to derive from five isolated text snippets. The LLM correctly refused, responding that it could not determine the highest return rate from the retrieved documents.

This is actually the expected and correct behaviour. It demonstrates exactly why RAG and Genie serve different purposes: RAG handles semantic lookup questions about specific products or vendors, while Databricks Genie handles SQL-style aggregation and ranking questions that require scanning the full dataset.
