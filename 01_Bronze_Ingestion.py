# Databricks notebook source
df = spark.read.option("header", True).csv(
    "/Volumes/vehicle_analytics/fuel_data/bronze/Vehicle-Fuel_consumption project.csv"
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

display(df.select("Fuel type").distinct())

# COMMAND ----------

display(df.select("Vehicle class").distinct())

# COMMAND ----------

display(df.select("Transmission").distinct())

# COMMAND ----------

display(
    df.select("Model year")
      .distinct()
      .orderBy("Model year")
)

# COMMAND ----------

import re

clean_df = df.toDF(*[
    re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", c)).strip("_")
    for c in df.columns
])

print(clean_df.columns)

# COMMAND ----------

clean_df.write.format("delta").mode("overwrite").saveAsTable(
    "vehicle_analytics.fuel_data.bronze_vehicle_fuel"
)

# COMMAND ----------

bronze_df = spark.table(
    "vehicle_analytics.fuel_data.bronze_vehicle_fuel"
)

print("Rows:", bronze_df.count())

bronze_df.printSchema()

# COMMAND ----------

silver_df = spark.table(
    "vehicle_analytics.fuel_data.bronze_vehicle_fuel"
)

display(silver_df.limit(5))