# 📊 Superstore Analytics Pipeline

End-to-end data pipeline project using Kaggle, PySpark, Airflow, MySQL and Metabase.

Made by Giuseppe
Made to test basic BI skills

Requirements are:
- PySpark
- Airflow (>= 3.0.0)
- MySQL (can be adapted to any SQL driver)
- Metabase (follow [this link](https://www.metabase.com/docs/latest/installation-and-operation/running-the-metabase-jar-file) to download local JAR Metabase)

Enjoy ;)

---

## 🚀 Architecture

Airflow → Spark → MySQL → Metabase

---

## 📦 Dataset

[Superstore Sales Dataset (Kaggle)](https://www.kaggle.com/datasets/jr2ngb/superstore-data)

---

## 🧱 Data Model

Star Schema:

- fact_table
- customer_data
- product_data
- region_data

---

## ⚙️ Pipeline

1. Data ingestion (Airflow)
2. Data validation
3. Data transformation (PySpark)
4. Data mart creation
5. Upload to MySQL 
6. Visualization (Metabase)

---

## 📊 KPI

- Top Customer by Profit per Segment
- Positive Profit Orders Sorted by Shipping Cost
- Total Shipping Cost by Order Priority
- Total Profit by Order Priority
- Monthly Profit Trends
- Total Profit by City

---
