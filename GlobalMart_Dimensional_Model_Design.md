# GlobalMart — Gold Layer: Dimensional Model Design

## Overview

The Silver layer gives us clean, consistent data — but clean data alone doesn't answer business questions. Business users think in terms of revenue by region, return rates by vendor, and which customers are most profitable. To support that, we needed to model the data into a structure that makes those queries fast and intuitive.

We went with a Star Schema. Two fact tables sit at the centre, connected to five dimension tables. The design was driven entirely by the three business failures we needed to solve: revenue audit, return fraud, and inventory blindspots.

---

## Fact Tables

### `fact_orders`

This is the primary fact table. Each row represents one line item within an order — a specific product purchased in a specific transaction.

- **Grain**: One row per order-product combination (transaction level)
- **Measures**: `sales`, `quantity`, `discount`, `profit`, `net_sales` (sales minus discount), `delivery_days` (actual delivery time), `delivery_variance_days` (actual vs estimated delivery)
- **Foreign Keys**: `customer_id` → `dim_customers`, `product_id` → `dim_products`, `vendor_id` → `dim_vendors`, `purchase_date_key` → `dim_date`

### `fact_returns`

This table captures every return event. By joining back to `fact_orders` and `dim_customers`, it powers both the fraud scoring logic and the vendor return rate analysis.

- **Grain**: One row per returned order invoice
- **Measures**: `refund_amount`, `days_to_return` (days between purchase date and return date)
- **Foreign Keys**: `customer_id` → `dim_customers`, `vendor_id` → `dim_vendors`, `return_date_key` → `dim_date`

---

## Dimension Tables

### `dim_customers`
- **Source**: `silver.customers`
- **Primary Key**: `customer_id`
- **Key Attributes**: `customer_name`, `customer_email`, `segment`, `city`, `state`, `country`, `postal_code`, `region`

### `dim_products`
- **Source**: `silver.products`
- **Primary Key**: `product_id`
- **Key Attributes**: `product_name`, `brand`, `primary_category`, `categories`, `upc`, `manufacturer`, `weight`, `dimension`

### `dim_vendors`
- **Source**: `silver.vendors`
- **Primary Key**: `vendor_id`
- **Key Attributes**: `vendor_name`

### `dim_date`
- **Source**: Generated — sequence of every date from `2014-01-01` to `2025-12-31`
- **Primary Key**: `date_key` (integer in `YYYYMMDD` format)
- **Key Attributes**: `year`, `quarter`, `month`, `month_name`, `day_of_week`, `is_weekend`, `fiscal_year`, `fiscal_quarter`, `year_quarter`

### `dim_geography`
- **Source**: Distinct location combinations from `silver.customers`
- **Primary Key**: `geo_key` (SHA-256 hash of city + state + postal_code)
- **Key Attributes**: `city`, `state`, `region`, `country`, `postal_code`

---

## How This Model Addresses the Three Business Failures

**Revenue Audit**: `fact_orders` brings transactions down to the product-line level and ties them to a unified customer dimension. With all six regional schemas harmonised in Silver, we can now calculate `net_sales` and `profit` across regions without the double-counting problem that came from isolated regional systems. The `vw_revenue_reconciliation` view built on top of this table actively flags orders where the same customer placed near-identical purchases on the same date.

**Return Fraud**: `fact_returns` joined with `dim_customers` gives fraud analysts a complete, unified view of each customer's return history across all regions — something that was impossible before because each region had its own isolated return system. The `days_to_return` derived measure and the link back to the original order are what power the anomaly scoring in the Returns Fraud Investigator.

**Inventory Blindspots**: `fact_orders` joined with `dim_products` and the customer's region tracks exactly what is selling where. The `vw_inventory_performance` view cross-joins every product against every region to expose the gaps — products that are moving in one region but have zero recorded sales in another. A full solution would also require a `fact_inventory_snapshot` table tracking warehouse stock levels, which is the logical next step.
