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


## 🥉 Bronze Layer
Raw CSV data from CRM & ERP is ingested into staging tables.

---

## 🥈 Silver Layer (Cleaned & Standardised)
Data is transformed into analytics-ready format:

- 🧹 Deduplication (latest records kept)
- 🧑‍🤝‍🧑 Standardised customer fields (gender, status)
- 📅 Date formatting → `YYYY-MM-DD`
- 📦 Consistent product categories & pricing
- 🔗 Data enriched via joins (CRM + ERP)

---

## 🥇 Gold Layer (Analytics Ready)
Star-schema built for reporting:

- `dim_customers` → enriched customer profiles
- `dim_products` → product catalog & metadata
- `fact_sales` → transactional sales data

---

## ⚙️ Key Skills
- SQL (joins, window functions, CTEs)
- ETL pipeline design (Bronze → Silver → Gold)
- Data cleaning & validation
- Star schema modelling
- Data warehousing best practices

