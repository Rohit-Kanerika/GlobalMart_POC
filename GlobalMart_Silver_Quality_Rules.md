# GlobalMart — Silver Layer: Data Quality Rules

## How We Used Databricks Genie During Bronze Review

Before writing any Silver transformation code, we used Databricks AI/BI Genie to run quick profiling queries against the Bronze tables. Instead of manually writing `SELECT DISTINCT` and `GROUP BY` queries for every column, we asked Genie things like *"Are there any transaction discount values outside the 0–1 range?"* or *"Show me all the different ship mode values across the three orders tables."* Genie generated the SQL instantly and returned the results, which gave us a fast way to spot inconsistencies we might have missed just by reading the file schemas.

The answers from those queries directly shaped the rules below — for example, the discount bounds check (0 to 1) came from Genie showing us discount values like `"40%"` and `1.5` appearing in the same column.

---

## Quality Rules by Table

| Table | Rule | What Triggers It | Consequence | Reasoning |
| :--- | :--- | :--- | :--- | :--- |
| customers | `customer_id` must not be null | `customer_id IS NULL` | Quarantine | Primary key — without it, the record cannot be joined to orders or transactions at all |
| customers | `customer_name` must not be null | `customer_name IS NULL` | Quarantine | Required for identity resolution and return fraud investigation briefs |
| customers | `segment` must be a valid value | Value not in `Consumer`, `Corporate`, `Home Office` | Quarantine | Downstream segment-level dashboards will produce incorrect groupings with invalid values |
| customers | `region` must be a valid value | Value not in `East`, `West`, `Central`, `South`, `North` | Quarantine | Regional attribution feeds the revenue reconciliation view directly |
| orders | `order_id` must not be null | `order_id IS NULL` | Quarantine | Primary key — needed to join to transactions and returns |
| orders | `customer_id` must not be null | `customer_id IS NULL` | Quarantine | An order without a customer cannot be attributed to any revenue segment |
| orders | `order_purchase_date` must not be null | `order_purchase_date IS NULL` | Quarantine | Date is required to generate the `purchase_date_key` for the fact table join to `dim_date` |
| orders | `ship_mode` must be a valid value | Value not in the standardised ship mode list | Quarantine | Fulfillment reporting requires consistent categorisation |
| orders | Date chronology must be logical | `purchase_date > approved_date` or `approved_date > carrier_date` | Flag (warn) | Likely a system timestamp glitch — we flag it for investigation but don't drop valid revenue |
| orders | `customer_id` must exist in `silver.customers` | FK not found | Flag (warn) | Orphan orders are worth flagging, but dropping them would undercount revenue |
| transactions | `order_id` must not be null | `order_id IS NULL` | Quarantine | Cannot assign revenue to an order without this link |
| transactions | `product_id` must not be null | `product_id IS NULL` | Quarantine | Product attribution is required for the inventory performance view |
| transactions | `sales` must be a positive number | `sales IS NULL` or `sales < 0` | Quarantine | Negative or missing revenue figures corrupt all downstream financial aggregations |
| transactions | `quantity` must be greater than zero | `quantity IS NULL` or `quantity <= 0` | Quarantine | A transaction must involve at least one physical unit |
| transactions | `discount` must be between 0 and 1 | `discount < 0` or `discount > 1` | Quarantine | Values outside this range indicate parsing failures from the `%`-suffixed strings in source files |
| returns | `order_id` must not be null | `order_id IS NULL` | Quarantine | Required to match the return to its original purchase for fraud scoring |
| returns | `refund_amount` must be positive | `refund_amount IS NULL` or `refund_amount <= 0` | Quarantine | A return without a monetary value cannot be counted toward the refund exposure metric |
| returns | `return_status` must be a valid value | Value not in `Approved`, `Rejected`, `Pending` | Quarantine | Invalid statuses break the fraud risk scoring logic in the Gold layer |
| products | `product_id` must not be null | `product_id IS NULL` | Quarantine | Primary key — required for all product dimension joins |
| products | `product_name` must not be null | `product_name IS NULL` | Quarantine | Product records without names are not usable by merchandising or catalog teams |
| vendors | No explicit rules applied | N/A | Pass-through | The vendor file is a small, clean reference table with no observed quality issues |
