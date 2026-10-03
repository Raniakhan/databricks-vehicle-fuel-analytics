# Vehicle Fuel Consumption & Emissions Analytics

## Project Overview
An end-to-end data engineering and analytics project using Databricks to analyze Canadian vehicle fuel consumption and CO2 emissions data. The project implements a Medallion Architecture to ingest, transform, and analyze 10,060 vehicle records to uncover environmental trends and vehicle efficiencies.

## Architecture
**Bronze → Silver → Gold**
*   **Bronze:** Ingested raw CSV data into a Delta table for immutable historical storage.
*   **Silver:** Performed data cleansing using PySpark. Standardized column names, handled missing/invalid values, and enforced schema integrity using safe casting.
*   **Gold:** Engineered five curated analytics tables using advanced Databricks SQL (CTEs, Window Functions, Joins) for business reporting and BI integration.

## Key Business Insights & SQL Implementation
*   **Emissions Trends (2015 vs 2024):** Utilized CTEs and `INNER JOIN`s to calculate the absolute change in average CO2 emissions per vehicle class over a 9-year period.
*   **Year-Over-Year Analysis:** Applied the `LAG()` window function to track yearly emission changes and fuel efficiency within specific vehicle classes.
*   **Class Rankings:** Leveraged `RANK()` and `ROW_NUMBER()` over partitioned data to isolate the highest-emitting vehicle classes per model year and extract the most recent vehicle data.

## Dashboard & Visualizations
Developed an interactive Databricks AI/BI dashboard featuring five visualizations and global filters to explore fuel consumption, vehicle classes, emissions, and environmental ratings. 

![Databricks AI/BI Dashboard](image_e1d0c5.png)

## Technologies Used
*   **Databricks Platform:** Serverless Compute, Databricks Jobs, Unity Catalog, AI/BI Dashboards
*   **Data Engineering:** PySpark, SQL, Delta Lake, Medallion Architecture (ETL/ELT)

## Orchestration & Performance
Configured a Databricks Job with three dependent notebook tasks:
1. Bronze ingestion
2. Silver transformation
3. Gold analytics

The automated workflow completed successfully with a measured end-to-end runtime of **2 minutes and 7 seconds (127 seconds)** on Serverless compute.

## Dataset & Project Structure
*Dataset:* Canadian vehicle fuel consumption ratings data (Original dataset excluded from repo per usage terms).

*   `01_Bronze_Ingestion.py`
*   `02_Silver_Transformation.py`
*   `03_Gold_Analytics.py`
