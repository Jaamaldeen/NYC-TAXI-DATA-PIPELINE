# 🚕 NYC Taxi Data Pipeline (SQL)

![NYC Taxi Data Pipeline Banner](<img width="1536" height="1024" alt="banner" src="https://github.com/user-attachments/assets/5f0afeff-b98e-494c-98f8-2fcac22c808f" />
)


This project implements an **end-to-end, automated data pipeline** for the public **NYC Taxi dataset**, built entirely using **SQL (PostgreSQL)**.  
It demonstrates a robust pipeline for **ingesting, cleaning, transforming, and aggregating** data, making it ready for BI and analytics.

The pipeline is designed to handle both **full historical loads** and **automated incremental loads**, following the modern **Medallion Architecture**.

---

## 🏛️ Architecture: The Medallion Model

The pipeline follows the **Bronze–Silver–Gold** layered data warehousing approach:

🥉 **Bronze Layer** – Stores the raw, untouched source data.  
🧩 A **UNIQUE constraint** ensures no duplicate records enter the pipeline.

🥈 **Silver Layer** – Performs all **data cleaning, standardization, transformation, and feature engineering**.

🥇 **Gold Layer** – The final **aggregated, business-ready layer**, optimized for reporting and analytics.  
It includes **five summary tables** for insights such as daily revenue, top vendors, payment types, and pickup zones.

---

![NYC Taxi Data Warehouse Diagram](<img width="1536" height="1024" alt="banner" src="https://github.com/user-attachments/assets/50f1d7f1-e91a-4a6b-990b-40ba6e525e79" />
)

---

## ⚙️ How It Works: Full vs Incremental Loads

### **1️⃣ Full Refresh (Historical Load)**  
The **`Full-load.sql`** script:
- Uses `COPY FROM` to load all raw CSVs into the Bronze layer.  
- Uses `CREATE TABLE AS (CTAS)` statements to transform and populate the Silver and Gold layers in a single pass.

### **2️⃣ Automated Incremental Load**  
This part ensures **only new data** is processed automatically.  
The only manual step is uploading new data into the **Bronze table** — everything else is trigger-based.

#### **Metadata Management**
A **`metadata.last_loading_period`** table logs the timestamp of each load.  
This drives intelligent, time-based incremental processing.

#### **Trigger 1 → Bronze → Silver**
An **AFTER INSERT trigger** on the Bronze table:
- Detects new records.
- Compares timestamps to the last update.
- Cleans, standardizes, and inserts only the new rows into Silver.

#### **Trigger 2 → Silver → Gold**
An **AFTER INSERT trigger** on the Silver table:
- Aggregates and appends new data into the **5 Gold summary tables**.

---

## 🛡️ Data Integrity

To maintain data quality and prevent duplication:
- A **UNIQUE constraint** is applied to `bronze.taxi_data`.  
- Any duplicate inserts are automatically rejected.  
- This ensures downstream Silver and Gold layers remain clean.

---

## 📁 Repository Structure

```bash
NYC-TAXI-DATA-PIPELINE/
├── sql_scripts/
│   ├── full_load.sql
│   ├── bronze_layer.sql
│   ├── silver_layer.sql
│   ├── gold_layer.sql
│   └── triggers/
│       ├── bronze_to_silver_trigger.sql
│       ├── silver_to_gold_trigger.sql
│
├── metadata/
│   └── last_loading_period.sql
│
├── images/
│   └── architecture.png
│
├── README.md
└── LICENSE 
```
## 🧰 Tech Stack

**Database:** PostgreSQL  
**Query Language:** SQL (CTAS, Triggers, Functions)  
**Data Source:** NYC Taxi Dataset (CSV files)  
**Data Storage:** Three-layer Medallion Architecture (Bronze, Silver, Gold)  
**Tools:** pgAdmin / DBeaver / SSMS (for testing and validation)  
**Version Control:** Git & GitHub  

---

## 💡 Key Features

🚀 **Fully SQL-based pipeline** – No external ETL tool or Python script required.  
⚙️ **Trigger-driven automation** – Incremental loads handled automatically via SQL triggers.  
🧩 **Metadata management** – Tracks load timestamps for efficient incremental updates.  
🛡️ **Duplicate prevention** – UNIQUE constraint ensures clean data at the source.  
🏗️ **Scalable architecture** – Modular Bronze–Silver–Gold design supports future growth.  
📊 **Analytics-ready outputs** – Gold layer includes summary tables for business KPIs.  

---

## 📈 Example Business Queries

These SQL queries highlight the insights derived from the **Gold Layer**:

```sql
-- 1️⃣ Daily Revenue Trends
SELECT trip_date, total_revenue
FROM gold.daily_summary
ORDER BY trip_date;

-- 2️⃣ Top Vendors by Revenue
SELECT vendor_name, total_revenue, avg_fare
FROM gold.vendor_summary
ORDER BY total_revenue DESC
LIMIT 5;

-- 3️⃣ Average Tip Percent by Payment Type
SELECT payment_description, avg_tip_percent
FROM gold.payment_summary
ORDER BY avg_tip_percent DESC;

-- 4️⃣ Monthly Performance Overview
SELECT month, total_trips, total_revenue
FROM gold.monthly_summary
ORDER BY month;

-- 5️⃣ Most Popular Pickup Zones
SELECT PULocationID, pickups, revenue_from_pickups
FROM gold.zone_summary
ORDER BY pickups DESC
LIMIT 10;
```

## 🤝 Feedback & Collaboration

I’m open to feedback, ideas, and collaboration opportunities.  
If you’d like to contribute or suggest improvements — such as integrating **Airflow**, **dbt**, or adding **Python-based orchestration** — feel free to get involved!  

You can:
- Open an [Issue](../../issues) to share feedback or report bugs  
- Submit a [Pull Request](../../pulls) with improvements  
- Connect with me on [LinkedIn](https://www.linkedin.com/in/jamaldeen-oyetunji-35a64718b) to discuss collaboration  

Your thoughts and contributions are always welcome — let’s build better data solutions together 🚀
