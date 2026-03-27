# GlobalMart — Bronze Layer: Data Exploration Summary

## What We Found During Exploration

Before writing any transformation code, we went through every Bronze table manually and catalogued all the structural and quality problems we found. The regional files were clearly built by different teams with no shared standard — column names, date formats, and even column ordering differed between regions.

Below is a summary of the main issues we found per entity.

### Schema & Structural Issues

**Customers** was the messiest entity. The `customer_id` column alone had four different names across the six regional files — `customer_id` in Region 1, `CustomerID` in Region 2, `cust_id` in Region 3, and `customer_identifier` in Region 6. Region 4's file had `city` and `state` in swapped column positions, which was easy to miss if you weren't looking at actual sample data. Region 5 was missing `customer_email` entirely — the column simply wasn't in the file.

**Orders** had inconsistent date formats. Region 1 used `MM/DD/YYYY HH:MM`, while Regions 2 and 3 used `YYYY-MM-DD`. Region 3 was also missing the `order_estimated_delivery_date` column. Ship mode values used different abbreviations across regions — `First Class` in one file, `1st Class` in another.

**Transactions** had a particularly tricky issue: the `discount` column was stored as a string with a percentage sign (e.g., `"40%"`) rather than a decimal number. A standard `cast(DoubleType())` blows up on this, which we discovered during initial Silver runs.

**Returns** came in two separate JSON files with different structures — one used a nested dict, the other an array. Both needed to be normalized before we could union them.

**Products** had minor issues — some null strings and inconsistent category formatting, but nothing that would break a pipeline.

**Vendors** was clean.

---

## How We Approached the Cleaning

Once we understood what was broken, we built the fixes into the Silver pipeline rather than patching data manually. The key decisions were:

- **Schema normalization via column mapping**: We defined a canonical column name dictionary for each entity and renamed all regional variants to the standard name at ingest time. This meant we didn't need to touch Bronze tables at all.
- **Structural fixes per region**: For Region 4, we explicitly swapped the `city` and `state` columns in the harmonization function. For Region 5, we injected a `NULL` email column so the schema aligned.
- **Safe casting for dirty types**: Instead of `cast()`, we used `try_cast` plus `regexp_replace` to strip the `%` character from discount values before converting. Any value that still failed got quarantined.
- **Quarantine pattern**: Any row that failed a quality check was written to a `*_quarantine` table alongside the actual reason it was rejected. Clean rows flowed into the master Silver table.

---

## Data Quality Register

| Entity | Issue | Source Region / File |
| :--- | :--- | :--- |
| customers | Column names inconsistent (`CustomerID`, `cust_id`, `customer_identifier`) | Regions 2, 3, 6 |
| customers | `city` and `state` columns swapped | Region 4 |
| customers | `customer_email` column missing entirely | Region 5 |
| customers | Segment and region values inconsistently cased or abbreviated | All regions |
| orders | Date format mismatch (`MM/DD/YYYY` vs `YYYY-MM-DD`) | Region 1 vs 2 & 3 |
| orders | `order_estimated_delivery_date` missing | Region 3 |
| orders | Ship mode abbreviations inconsistent | Regions 1, 2, 3 |
| transactions | Discount values stored as strings with `%` symbol | All regions |
| returns | JSON structure varies (dict vs array) | Region 6 vs global file |
| products | Null strings and inconsistent category formatting | Global (`products.json`) |
| vendors | No structural issues found | Global (`vendors.csv`) |

---

## AI Assistance During Exploration

We used Databricks AI/BI Genie to speed up the profiling phase. Rather than writing SQL profiling queries manually for each table, we pointed Genie at the Bronze schema and asked questions like *"Are there any discount values greater than 1 in the transactions table?"* and *"What are all the distinct ship mode values across the orders tables?"*. This let us identify edge cases quickly and focus our time on writing the actual fixes.
