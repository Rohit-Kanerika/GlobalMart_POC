# GlobalMart Databricks Hackathon POC

Welcome to the GlobalMart Proof of Concept (POC) repository. This project demonstrates an end-to-end data engineering, analytics, and AI lifecycle built entirely on the Databricks Data Intelligence Platform. 

## 🏗️ Medallion Data Architecture

Our data pipelines (`/notebooks`) strictly follow the Databricks Medallion Architecture, progressing raw multi-region retail data through progressive stages of refinement. The notebooks are numerically ordered to represent the exact execution workflow.

### 🥉 Bronze Layer: Raw Data Ingestion
* **`01_bronze_ingestion.py`**: This notebook serves as the entry point. It ingests raw CSV and JSON files representing regional sales, customers, transactions, and returns from the `/9a85aa8b-4ae2-4d92-83ae-778e4774d98c_GlobalMartRetailData` directory into our Bronze Delta tables, maintaining the original schema and preserving a raw history.

### 🥈 Silver Layer: Cleansing & Conforming
The Silver layer pipelines clean the raw data by enforcing data types, standardizing date formats, handling missing values, and applying strict data quality constraints.
* **`02_silver_customers.py`**: Deduplicates and unifies customer profiles across the various regions.
* **`03_silver_orders.py`**: Standardizes order timestamps and line-item details.
* **`04_silver_transactions.py`**: Cleanses financial transactional records, removing or fixing invalid casting and monetary anomalies.
* **`05_silver_returns.py`**: Processes JSON-based return data to align with structured downstream facts.
* **`06_silver_products_vendors.py`**: Unifies static operational data formats (Products and Vendors).

### 🥇 Gold Layer: Business-Level Aggregates (Star Schema)
The Gold layer models the cleansed data into a highly optimized Dimensional Model (Star Schema) tailored for business intelligence, reporting, and AI.
* **`07_gold_dimensions.py`**: Generates pure dimension tables (e.g., `dim_customer`, `dim_product`, `dim_date`).
* **`08_gold_facts.py`**: Generates centralized fact tables (e.g., `fact_sales`, `fact_returns`).
* **`09_gold_analytics.py`**: Pre-calculates specific business analytics and materialized views for high-performance dashboards.
* **`gold_layer_merged.py`**: An aggregated script encompassing the entire Gold layer creation logic.

---

## 🤖 AI, BI & Advanced Analytics

Beyond standard data engineering, this POC integrates several AI and Generative AI workflows directly on top of the Gold layer:
* **`10_ai_genie_setup.py`**: Configuration for Databricks Genie, allowing natural language querying over the Gold tables.
* **`11_ai_insights.py` & `15_ai_business_insights.py`**: Notebooks dedicated to extracting predictive and statistical business insights from the curated data.
* **`12_dq_audit_report.py`**: An automated data quality auditing framework and report generator.
* **`13_return_fraud_investigator.py`**: An analytical tool designed to detect patterned anomalies and flag highly probable fraudulent returns.
* **`14_rag_product_qa.py`**: A Retrieval-Augmented Generation (RAG) system built to answer complex questions regarding GlobalMart products by vectorizing product metadata and manuals.

---

## 📄 Design & Project Documentation

To ensure full transparency into the engineering decisions and business logic, the repository includes comprehensive documentation:

* **Analytical & Architectural Designs**
  * [`GlobalMart_Dimensional_Model_Design.md`](./GlobalMart_Dimensional_Model_Design.md): A detailed ERD/layout of the Star Schema designed for the Gold layer.
  * [`db_architecture.png`](./db_architecture.png): A comprehensive diagram showcasing the data flow from source ingestion through the Medallion layers and out to the AI/BI applications.

* **Data Quality & Exploration**
  * [`GlobalMart_Data_Exploration_Summary.md`](./GlobalMart_Data_Exploration_Summary.md): Initial exploratory data analysis (EDA) findings, highlighting raw data anomalies, distributions, and structural characteristics discovered before the Silver layer.
  * [`GlobalMart_Silver_Quality_Rules.md`](./GlobalMart_Silver_Quality_Rules.md): The specific data governance, validation checks, and data quality constraints implemented to promote Bronze to Silver.
  * [`a47939d9-2556..._GlobalMartDataDictionary.xlsx`](./a47939d9-2556-4833-a8b0-95980ec75549_GlobalMartDataDictionary.xlsx): The complete data dictionary mapping fields and types.

* **POC Deliverables & Answers**
  * `GlobalMart_Gold_Queries.md`: Example high-performance Spark SQL queries executing against the newly modeled Gold data.
  * `GlobalMart_Returns_Fraud_Answers.md`: Deep-dive rationale behind our approach to identifying return fraud.
  * `GlobalMart_DQ_Reporter_Answers.md`: Responses focused on the implementation of our data quality reporting mechanism.
  * `GlobalMart_RAG_Genie_Answers.md`: Architectural decisions behind setting up our RAG and Genie features.
  * `GlobalMart_Final_Assessment.md` / `GlobalMart_Team_Summary.md`: The concluding project retrospective and final assessment of the solution.

---

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/Rohit-Kanerika/GlobalMart_POC.git
```

### 2. Upload to Databricks Workspace
1. Navigate to your Databricks Workspace.
2. Select **Workspace > Users > [Your Username]**.
3. Import the `/notebooks` folder directly into your workspace environment.

### 3. Pipeline Execution
1. Ensure a cluster (Standard or Serverless) is running and attached to the notebooks.
2. The raw files inside `/9a85aa8b...GlobalMartRetailData/` should be placed into your designated cloud storage Volume or DBFS location that the `01_bronze_ingestion.py` notebook points to.
3. **Execute sequentially:** Run the notebooks in numerical order (`01` through `15`). Each notebook dynamically relies on the Delta tables processed and published by the previous step.
