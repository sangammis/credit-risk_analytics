## Project Overview
This project demonstrates an end-to-end Credit Risk Analytics Platform built using modern Data Engineering and Lakehouse architecture principles. The solution ingests loan customer data into AWS S3, processes it using Databricks and Apache Spark, transforms it through Delta Lake Bronze, Silver, and Gold layers,loads curated datasets into Snowflake, and delivers business insights through interactive Power BI dashboards.

The platform enables risk segmentation, default prediction, and portfolio monitoring to support data-driven lending decisions.

## Business Objectives

-Identify high-risk borrower segment.s
-Improve portfolio risk management.
-Enable risk-based lending decisions.
-Monitor default trends and portfolio health.
-Provide actionable business insights through analytics dashboards.

## Technologies Used

Data Engineering

-Databricks
-Apache Spark (PySpark)
-Delta Lake
-Snowflake

Cloud Services

-AWS S3
-AWS Glue
-Amazon Athena

Analytics & Reporting

-Python
-SQL
-Power BI

Data Processing

-Pandas
-NumPy

## Data Pipeline

Bronze Layer

-Raw loan customer data stored in Amazon S3
-Initial ingestion into Delta Lake Bronze tables

Silver Layer

-Data cleansing and validation
-Schema enforcement
-Null handling and standardization
-Data quality checks

## Feature Engineering

Created business-focused credit risk features:

-Debt-to-Income (DTI) Ratio.
-Credit Utilization Ratio.
-Risk Bands.
-Loan-to-Income Ratio.
-Customer Risk Categories.

Gold Layer

-Aggregated and business-ready datasets
-Risk analytics tables
-Portfolio performance metrics

Snowflake Analytics Layer

-Curated datasets loaded into Snowflake
-SQL-based risk analysis and reporting
-Optimized analytical queries

## Key Analysis Performed

-Data ingestion and ETL processing
-Data cleansing and transformation
-Feature engineering
-Credit risk segmentation
-Default trend analysis
-Portfolio risk monitoring
-Business KPI generation

## Key Insights

-Contract employees exhibit significantly higher default rates than salaried customers.
-Borrowers with credit scores below 600 demonstrate substantially increased default risk.
-Higher debt-to-income and credit utilization ratios strongly correlate with loan defaults.
-Risk-band segmentation enables proactive identification of high-risk customer groups.
-Portfolio-level monitoring helps improve lending and approval strategies.

## Power BI Dashboard Preview
![Credit Risk Dashboard](powerbi/Screenshot01.png)
![Credit Risk Dashboard](powerbi/Screenshot02.png)
![Credit Risk Dashboard](powerbi/Screenshot03.png)
![Credit Risk Dashboard](powerbi/Screenshot04.png)
  
## Business Impact

-Built a scalable end-to-end data platform using AWS, Databricks, Delta Lake, Snowflake, and Power BI.
-Automated data ingestion, transformation, and analytics workflows.
-Improved visibility into borrower risk profiles through engineered risk features.
-Enabled data-driven lending decisions through portfolio-level risk monitoring.
-Delivered interactive dashboards for executive and operational reporting.

  
