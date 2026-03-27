# GlobalMart — UC1: DQ Audit Reporter

## Implementation Notes

### Which quarantine tables did you read from, and what issue types did you find?

The notebook reads from four Silver quarantine tables: `silver.customers_quarantine`, `silver.orders_quarantine`, `silver.transactions_quarantine`, and `silver.returns_quarantine`. Each of these is populated by the Silver pipeline when a record fails a quality check — the record is routed there along with a `_dq_failure_reasons` column explaining why it was rejected.

When we grouped those records by failure reason, the issues we found were:
- **Customers**: `customer_id is NULL`, `invalid segment: NULL`
- **Transactions**: `invalid sales value` (negative or null sales amounts)
- **Orders**: `order_id is NULL`, date chronology violations
- **Returns**: `refund_amount is NULL or <= 0`

---

### How did you structure your prompt?

The key design decision was to group rejections by issue type before calling the LLM at all. There's no value in sending the model 47 identical rows that all failed for the same reason — you get a better explanation by telling it "47 records failed because of this one issue" and asking it to explain the business impact of that specific issue.

The prompt passes three variables: the entity name, the issue type string, and the rejection count. It asks the model to answer three specific questions without bullet points or filler:

```
You are a Data Governance AI assisting the GlobalMart Finance team with an external audit.
We rejected {count} records from the '{entity}' pipeline due to the following data quality issue:
"{issue_type}"

Write a plain-English explanation (3-4 sentences maximum) addressing:
1. What the problem is and what pattern triggered the rejection.
2. Why this specific record type cannot be accepted into the analytics layer.
3. What specific business report, audit figure, or operational decision is at risk if this data were included.

Do not use bullet points. State the facts clearly and professionally.
```

The instruction to avoid bullet points was deliberate — the Finance team reads these as paragraphs, not lists.

---

### Evaluating two LLM explanations from `gold.dq_audit_report`

**Explanation 1 — Transactions: `invalid sales value` (23 rows)**

> "During the audit, 23 records were rejected from the transactions pipeline because the sales value fell below zero or was entirely missing. Transactions without a valid, positive monetary signature cannot be accepted into the analytic layer because they violate core accounting principles. If these records were included, the quarterly Gross Revenue reporting figure would be artificially skewed or computationally broken, placing the integrity of the external financial audit at immediate risk."

This one works well. It names the field (sales), states the count (23), and identifies the exact report at risk (quarterly Gross Revenue).

**Explanation 2 — Customers: `invalid segment: NULL` (47 rows)**

> "A total of 47 records from the customers pipeline were quarantined due to an invalid or missing customer segment classification. Dimensional customer records lacking segment data cannot be integrated because they break the hierarchy required for demographic rollups. Including this unclassified data would severely compromise the Regional Profitability by Segment report, preventing marketing teams from making data-backed operational budgeting decisions."

Again, all three criteria are met — field (segment), count (47), and business risk (Regional Profitability by Segment report).

**Where the prompt could be improved**: The phrase `"invalid sales value"` doesn't tell the LLM whether the value was negative, null, or a non-numeric string. If we made the Silver pipeline inject the actual bad value into the failure reason string — something like `"sales value was negative: -12.50"` — the LLM explanation would be more precise and more useful to an auditor looking at the actual problem.
