# GlobalMart — Gold Layer: Pre-Aggregated Analytics Views

## Why Pre-Compute These Views?

The Gold layer exists to make business queries fast and reliable. The three analytics views we built are not convenience shortcuts — they address specific computational problems that would make dashboards unusably slow if run fresh on every page load.

Below is a walkthrough of each view, what it measures, and why it's a good candidate for pre-computation.

---

## 1. Revenue Reconciliation (`vw_revenue_reconciliation`)

**What it measures**: Gross revenue, net revenue, total profit, and order volume broken down by customer, region, and fiscal quarter. Critically, it also computes a `potential_duplicate_flag` on each order — if the same customer placed another order on the same date for a revenue amount within 5% of this one, it gets flagged.

**Business failure it addresses**: Revenue Audit. The Finance team needed a way to surface double-booked or near-duplicate transactions that were slipping through the regional ingestion systems undetected.

**Why pre-compute it**: Detecting duplicates requires a correlated subquery or self-join across the entire `fact_orders` history — for every row, the database has to look for another row matching on customer, date, and revenue proximity. On a table with millions of rows, running this on-demand every time a dashboard refreshes is prohibitively expensive. Pre-computing the boolean flag means the dashboard just filters on a column.

---

## 2. Customer Return Profile (`vw_customer_return_profile`)

**What it measures**: The complete return history per customer — total returns, approved vs rejected counts, total and average refund amounts, and the time span between a customer's first and most recent return. All of this feeds into a tiered `fraud_risk_score` (1–5) and a `fraud_risk_label` like `HIGH - Immediate Review Required`.

**Business failure it addresses**: Return Fraud. Before this view existed, each region had its own return records and no team had visibility across all six systems. A customer could systematically abuse the return policy across multiple regions without ever appearing suspicious in any single region's data.

**Why pre-compute it**: Scoring requires aggregating `MAX` and `MIN` return dates per customer, conditional counting for each status type, and a multi-condition `CASE WHEN` evaluation. Running this live for a fraud analyst dashboard adds latency that doesn't need to be there. Pre-computing it means the fraud team can run `WHERE fraud_risk_score = 5` and get results instantly.

---

## 3. Inventory Performance (`vw_inventory_performance`)

**What it measures**: Units sold, revenue, and an estimated weekly unit velocity for every product in every region. The view also raises an `inventory_blindspot_flag` for any product-region combination where the product sells elsewhere but has zero recorded sales in that region.

**Business failure it addresses**: Inventory Blindspot. Zero sales in a region could mean the product isn't stocked there or it's simply not selling. This view surfaces those gaps so the merchandising team can investigate.

**Why pre-compute it**: Finding the *absence* of sales requires generating every possible product-region combination first via a `CROSS JOIN`, then left-joining actual sales to find the nulls. Cartesian products are expensive at scale — we have hundreds of products and six regions, which is manageable, but running the cross join dynamically on every dashboard query is wasteful. Pre-computing it in the nightly pipeline keeps query times near-instant for the end user.
