# Data Warehouse ETL Project

## Overview
This project demonstrates a **robust end-to-end ETL pipeline** for a data warehouse, transforming raw operational data into analytics-ready datasets. It highlights my expertise in **SQL, data pipeline design, database architecture, and data engineering best practices**.

The pipeline follows a **layered architecture**:

- **Bronze Layer:** Raw data ingestion from multiple source systems (CRM & ERP).  
- **Silver Layer:** Cleansing, standardization, and transformation to ensure data quality and consistency.  
- **Gold Layer:** Business-level dimension and fact tables, structured for analytics and reporting.  

This project showcases building scalable and maintainable **data pipelines** for real-world enterprise data.

---

## Architecture & Data Flow

The project follows a **three-tier architecture**:

![Data Architecture](https://github.com/BenDatta/data_warehouse_etl/blob/main/docs/data_architecture.png)

**Data Flow Overview:**

![Data Flow](https://github.com/BenDatta/data_warehouse_etl/blob/main/docs/data_flow.png)

1. **Bronze Layer:** Raw CSV files from CRM and ERP are ingested into corresponding tables.  
2. **Silver Layer:** Data is cleaned, standardized, and enriched:
    - Deduplicates records based on the latest entry.
    - Standardizes fields like gender and marital status.
    - Converts date formats to `YYYY-MM-DD`.
    - Ensures product categories and prices are consistent.  
3. **Gold Layer:** Dimension and fact tables created for analytical consumption:
    - `dim_customers` – Enriched customer information with demographics.
    - `dim_products` – Product catalog with categories, lines, and costs.
    - `fact_sales` – Transactional sales data linked to customers and products.

---

## Key Skills Demonstrated

- **SQL Proficiency:** Writing complex transformations, window functions, joins, and conditional logic.  
- **Data Pipeline Development:** Implemented multi-layered ETL from raw source to business-ready views.  
- **Database Design:** Structured data into Bronze, Silver, and Gold schemas with surrogate keys and relationships.  
- **Data Quality & Governance:** Applied deduplication, validation, and auditing strategies.  
- **Analytical Modeling:** Created star-schema design with dimensions (`dim_customers`, `dim_products`) and facts (`fact_sales`) for reporting.

---

## Silver Layer Highlights

Transforms raw Bronze data into **high-quality, analytics-ready datasets**:

- **Customer Data:** Cleans names, standardizes gender and marital status, keeps the latest record per customer.  
- **Product Data:** Generates category IDs, standardizes product lines, handles missing costs and invalid dates.  
- **Sales Data:** Cleans order, shipping, and due dates; validates sales amount and price consistency.  
- **ERP Data:** Standardizes location and demographic data for integration with CRM.

**Techniques applied:**  

- Window functions (`ROW_NUMBER`) for deduplication  
- Conditional transformations (`CASE WHEN`)  
- Data type conversions and validation  
- Joining multiple source tables for enrichment

---

## Gold Layer Highlights

Provides **business-level views** ready for analytics:

- **`dim_customers`**
    - Enriched customer profiles combining CRM & ERP data
    - Surrogate keys, demographics, and geographic information

- **`dim_products`**
    - Product metadata, categories, and costs
    - Filters historical products for active reporting

- **`fact_sales`**
    - Links sales transactions to customers and products
    - Captures order, shipping, due dates, quantities, and sales metrics

Demonstrates **data modeling skills** and ability to build a **star schema** optimized for analytics.

---

## Project Workflow

1. **Load Bronze Layer**
    - Raw CSVs ingested into Bronze tables.
    - Load operations logged to monitor success/failure.

2. **Transform to Silver Layer**
    - Cleanse, deduplicate, and standardize data.
    - Handle missing and inconsistent values.

3. **Create Gold Views**
    - Generate business-ready dimension and fact tables.
    - Ready for analytics, BI dashboards, or reporting.

---

## Achievements & Takeaways

- Implemented a **full ETL pipeline** using SQL without external tools.  
- Built **auditable pipelines** with logging and error tracking.  
- Designed **scalable and maintainable database structures** for analytics.  
- Hands-on experience in **multi-layered data warehousing** (Bronze → Silver → Gold).  
