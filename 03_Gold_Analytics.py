# Databricks notebook source
gold_df = spark.table(
    "vehicle_analytics.fuel_data.silver_vehicle_fuel"
)

gold_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business Question:
# MAGIC -- Which vehicle classes have the lowest average fuel consumption
# MAGIC -- and CO2 emissions, and how many vehicles are recorded in each class?
# MAGIC SELECT
# MAGIC     Vehicle_class,
# MAGIC     COUNT(*) AS total_vehicles,
# MAGIC     ROUND(AVG(Combined_L_100_km), 2) AS avg_fuel_consumption,
# MAGIC     ROUND(AVG(CO2_emissions_g_km), 2) AS avg_co2_emissions
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC GROUP BY Vehicle_class
# MAGIC ORDER BY avg_fuel_consumption ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fuel efficiency trends by year
# MAGIC SELECT
# MAGIC     Model_year,
# MAGIC     COUNT(*) AS total_vehicles,
# MAGIC     ROUND(AVG(Combined_L_100_km), 2)
# MAGIC         AS avg_fuel_consumption,
# MAGIC     ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC         AS avg_co2_emissions
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC GROUP BY Model_year
# MAGIC ORDER BY Model_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Which vehicle classes have more than 500 records and average CO₂ emissions above 250 g/km, considering only vehicles from 2020 onward?
# MAGIC */
# MAGIC SELECT
# MAGIC     Vehicle_class,
# MAGIC     COUNT(*) AS total_vehicles,
# MAGIC     ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC         AS avg_co2_emissions
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC WHERE Model_year >= 2020
# MAGIC GROUP BY Vehicle_class
# MAGIC HAVING COUNT(*) > 500
# MAGIC    AND AVG(CO2_emissions_g_km) > 250
# MAGIC ORDER BY avg_co2_emissions DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How does average CO₂ emissions for each vehicle class compare between 2015 and 2024?
# MAGIC WITH emissions_2015 AS (
# MAGIC     SELECT
# MAGIC         Vehicle_class,
# MAGIC         ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC             AS avg_co2_2015
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     WHERE Model_year = 2015
# MAGIC     GROUP BY Vehicle_class
# MAGIC ),
# MAGIC
# MAGIC emissions_2024 AS (
# MAGIC     SELECT
# MAGIC         Vehicle_class,
# MAGIC         ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC             AS avg_co2_2024
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     WHERE Model_year = 2024
# MAGIC     GROUP BY Vehicle_class
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     a.Vehicle_class,
# MAGIC     a.avg_co2_2015,
# MAGIC     b.avg_co2_2024,
# MAGIC     ROUND(b.avg_co2_2024 - a.avg_co2_2015, 2)
# MAGIC         AS emissions_difference
# MAGIC FROM emissions_2015 a
# MAGIC INNER JOIN emissions_2024 b
# MAGIC     ON a.Vehicle_class = b.Vehicle_class
# MAGIC ORDER BY emissions_difference DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business Question:
# MAGIC -- Which vehicle classes have the highest average CO2
# MAGIC -- emissions within each model year?
# MAGIC WITH yearly_emissions AS (
# MAGIC     SELECT
# MAGIC         Model_year,
# MAGIC         Vehicle_class,
# MAGIC         ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC             AS avg_co2_emissions
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     GROUP BY Model_year, Vehicle_class
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     Model_year,
# MAGIC     Vehicle_class,
# MAGIC     avg_co2_emissions,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY Model_year
# MAGIC         ORDER BY avg_co2_emissions DESC
# MAGIC     ) AS emissions_rank
# MAGIC
# MAGIC FROM yearly_emissions
# MAGIC ORDER BY Model_year, emissions_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business Question:
# MAGIC -- How did average CO2 emissions for each vehicle class
# MAGIC -- change compared with the previous available model year?
# MAGIC WITH yearly_emissions AS (
# MAGIC     SELECT
# MAGIC         Model_year,
# MAGIC         Vehicle_class,
# MAGIC         ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC             AS avg_co2_emissions
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     GROUP BY Model_year, Vehicle_class
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     Model_year,
# MAGIC     Vehicle_class,
# MAGIC     avg_co2_emissions,
# MAGIC
# MAGIC     LAG(avg_co2_emissions) OVER (
# MAGIC         PARTITION BY Vehicle_class
# MAGIC         ORDER BY Model_year
# MAGIC     ) AS previous_year_emissions,
# MAGIC
# MAGIC     ROUND(
# MAGIC         avg_co2_emissions -
# MAGIC         LAG(avg_co2_emissions) OVER (
# MAGIC             PARTITION BY Vehicle_class
# MAGIC             ORDER BY Model_year
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS year_over_year_change
# MAGIC
# MAGIC FROM yearly_emissions
# MAGIC ORDER BY Vehicle_class, Model_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business Question:
# MAGIC -- What is the most recent model year available for each
# MAGIC -- vehicle class, and what are its average fuel consumption
# MAGIC -- and CO2 emissions?
# MAGIC WITH yearly_summary AS (
# MAGIC     SELECT
# MAGIC         Model_year,
# MAGIC         Vehicle_class,
# MAGIC         COUNT(*) AS total_vehicles,
# MAGIC         ROUND(AVG(Combined_L_100_km), 2)
# MAGIC             AS avg_fuel_consumption,
# MAGIC         ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC             AS avg_co2_emissions
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     GROUP BY Model_year, Vehicle_class
# MAGIC ),
# MAGIC
# MAGIC ranked_data AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Vehicle_class
# MAGIC             ORDER BY Model_year DESC
# MAGIC         ) AS row_num
# MAGIC     FROM yearly_summary
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     Model_year,
# MAGIC     Vehicle_class,
# MAGIC     total_vehicles,
# MAGIC     avg_fuel_consumption,
# MAGIC     avg_co2_emissions
# MAGIC FROM ranked_data
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY Vehicle_class;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     vehicle_analytics.fuel_data.gold_fuel_by_vehicle_class
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC -- Business Question:
# MAGIC -- Which vehicle classes have the lowest average fuel
# MAGIC -- consumption and CO2 emissions, and how many vehicles
# MAGIC -- are recorded in each class?
# MAGIC
# MAGIC SELECT
# MAGIC     Vehicle_class,
# MAGIC     COUNT(*) AS total_vehicles,
# MAGIC     ROUND(AVG(Combined_L_100_km), 2)
# MAGIC         AS avg_fuel_consumption,
# MAGIC     ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC         AS avg_co2_emissions
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC GROUP BY Vehicle_class
# MAGIC ORDER BY avg_fuel_consumption ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vehicle_analytics.fuel_data.gold_fuel_by_vehicle_class
# MAGIC ORDER BY avg_fuel_consumption ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     vehicle_analytics.fuel_data.gold_yearly_trends
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC -- Business Question:
# MAGIC -- How have average fuel consumption and CO2 emissions
# MAGIC -- changed across model years?
# MAGIC
# MAGIC SELECT
# MAGIC     Model_year,
# MAGIC     COUNT(*) AS total_vehicles,
# MAGIC     ROUND(AVG(Combined_L_100_km), 2)
# MAGIC         AS avg_fuel_consumption,
# MAGIC     ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC         AS avg_co2_emissions
# MAGIC
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC
# MAGIC GROUP BY Model_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vehicle_analytics.fuel_data.gold_yearly_trends
# MAGIC ORDER BY Model_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     vehicle_analytics.fuel_data.gold_vehicle_class_comparison
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC -- Business Question:
# MAGIC -- How does average CO2 emissions for each vehicle
# MAGIC -- class differ between 2015 and 2024?
# MAGIC
# MAGIC WITH emissions_2015 AS (
# MAGIC     SELECT
# MAGIC         Vehicle_class,
# MAGIC         AVG(CO2_emissions_g_km) AS co2_2015
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     WHERE Model_year = 2015
# MAGIC     GROUP BY Vehicle_class
# MAGIC ),
# MAGIC
# MAGIC emissions_2024 AS (
# MAGIC     SELECT
# MAGIC         Vehicle_class,
# MAGIC         AVG(CO2_emissions_g_km) AS co2_2024
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC     WHERE Model_year = 2024
# MAGIC     GROUP BY Vehicle_class
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     a.Vehicle_class,
# MAGIC     ROUND(a.co2_2015, 2) AS avg_co2_2015,
# MAGIC     ROUND(b.co2_2024, 2) AS avg_co2_2024,
# MAGIC     ROUND(b.co2_2024 - a.co2_2015, 2)
# MAGIC         AS co2_change
# MAGIC
# MAGIC FROM emissions_2015 a
# MAGIC
# MAGIC INNER JOIN emissions_2024 b
# MAGIC     ON a.Vehicle_class = b.Vehicle_class;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vehicle_analytics.fuel_data.gold_vehicle_class_comparison
# MAGIC ORDER BY co2_change DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     vehicle_analytics.fuel_data.gold_emissions_rankings
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC -- Business Question:
# MAGIC -- Which vehicle classes have the highest average
# MAGIC -- CO2 emissions within each model year?
# MAGIC
# MAGIC SELECT
# MAGIC     Model_year,
# MAGIC     Vehicle_class,
# MAGIC     ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC         AS avg_co2_emissions,
# MAGIC
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY Model_year
# MAGIC         ORDER BY AVG(CO2_emissions_g_km) DESC
# MAGIC     ) AS emissions_rank
# MAGIC
# MAGIC FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC
# MAGIC GROUP BY
# MAGIC     Model_year,
# MAGIC     Vehicle_class;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vehicle_analytics.fuel_data.gold_emissions_rankings
# MAGIC WHERE emissions_rank <= 3
# MAGIC ORDER BY Model_year, emissions_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC     vehicle_analytics.fuel_data.gold_latest_vehicle_class
# MAGIC USING DELTA
# MAGIC AS
# MAGIC
# MAGIC -- Business Question:
# MAGIC -- What is the most recent model year available
# MAGIC -- for each vehicle class, and its average fuel
# MAGIC -- consumption and CO2 emissions?
# MAGIC
# MAGIC WITH yearly_summary AS (
# MAGIC     SELECT
# MAGIC         Vehicle_class,
# MAGIC         Model_year,
# MAGIC
# MAGIC         ROUND(AVG(Combined_L_100_km), 2)
# MAGIC             AS avg_fuel_consumption,
# MAGIC
# MAGIC         ROUND(AVG(CO2_emissions_g_km), 2)
# MAGIC             AS avg_co2_emissions
# MAGIC
# MAGIC     FROM vehicle_analytics.fuel_data.silver_vehicle_fuel
# MAGIC
# MAGIC     GROUP BY
# MAGIC         Vehicle_class,
# MAGIC         Model_year
# MAGIC ),
# MAGIC
# MAGIC ranked_data AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Vehicle_class
# MAGIC             ORDER BY Model_year DESC
# MAGIC         ) AS row_num
# MAGIC     FROM yearly_summary
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     Vehicle_class,
# MAGIC     Model_year AS latest_model_year,
# MAGIC     avg_fuel_consumption,
# MAGIC     avg_co2_emissions
# MAGIC
# MAGIC FROM ranked_data
# MAGIC WHERE row_num = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vehicle_analytics.fuel_data.gold_latest_vehicle_class
# MAGIC ORDER BY Vehicle_class;

# COMMAND ----------

# DBTITLE 1,Show Tables in Fuel Data Schema
# MAGIC %sql
# MAGIC SHOW TABLES IN vehicle_analytics.fuel_data;