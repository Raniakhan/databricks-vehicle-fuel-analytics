# Vehicle Fuel Consumption Analytics

## Project Overview
An end-to-end data engineering and analytics project using Databricks to analyze Canadian vehicle fuel consumption and CO2 emissions data.

The project implements a Medallion Architecture to ingest, transform, and analyze 10,060 vehicle records.

## Architecture

Bronze → Silver → Gold

- **Bronze:** Ingested raw CSV data into a Delta table.
- **Silver:** Cleaned column names, handled missing and invalid values, and standardized data types using safe casting.
- **Gold:** Created five analytics tables for business reporting and insights.

## Technologies Used
- Databricks
- PySpark and SQL
- Delta Lake
- Unity Catalog
- Databricks Jobs and scheduled workflows
- Databricks AI/BI Dashboards

## Gold Analytics Tables
1. `gold_fuel_by_vehicle_class`
2. `gold_yearly_trends`
3. `gold_vehicle_class_comparison`
4. `gold_emissions_rankings`
5. `gold_latest_vehicle_class`

## Dashboard
Developed an interactive dashboard with five visualizations and filters for exploring fuel consumption, vehicle classes, emissions, and environmental ratings.

## Orchestration
Configured a Databricks Job with three dependent notebook tasks:

1. Bronze ingestion
2. Silver transformation
3. Gold analytics

The workflow completed successfully with a measured end-to-end runtime of 2 minutes and 7 seconds (127 seconds) on Serverless compute.

## Dataset
Canadian vehicle fuel consumption ratings data.

The original dataset is not included in this repository. Please refer to the original dataset source for access and usage terms.

## Project Structure
- `01_Bronze_Ingestion.py`
- `02_Silver_Transformation.py`
- `03_Gold_Analytics.py`
- `README.md`

## Author
Rania Khan
