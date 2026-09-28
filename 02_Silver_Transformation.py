# Databricks notebook source
silver_df = spark.table(
    "vehicle_analytics.fuel_data.bronze_vehicle_fuel"
)
display(silver_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = silver_df.withColumn(
    "Model_year",
    col("Model_year").cast("int")
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "Engine_size_L",
    col("Engine_size_L").cast("double")
)
silver_df.printSchema()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "Cylinders",
    col("Cylinders").cast("int")
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

numeric_columns = { "Model_year": "int",
    "Engine_size_L": "double",
    "Cylinders": "int",
    "City_L_100_km": "double",
    "Highway_L_100_km": "double",
    "Combined_L_100_km": "double",
    "Combined_mpg": "double",
    "CO2_emissions_g_km": "int",
    "CO2_rating": "int",
    "Smog_rating": "int"
}

for column_name, data_type in numeric_columns.items():
    silver_df = silver_df.withColumn(
        column_name,
        col(column_name).cast(data_type)
    )

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr

for column_name, data_type in numeric_columns.items():
    silver_df = silver_df.withColumn(
        column_name,
        expr(f"try_cast(`{column_name}` AS {data_type})")
    )

# COMMAND ----------

from pyspark.sql.functions import expr

# Reload a fresh copy from Bronze
silver_df = spark.table(
    "vehicle_analytics.fuel_data.bronze_vehicle_fuel"
)

# Convert all numeric columns safely
for column_name, data_type in numeric_columns.items():
    silver_df = silver_df.withColumn(
        column_name,
        expr(f"try_cast(`{column_name}` AS {data_type})")
    )

display(silver_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    silver_df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in silver_df.columns
    ])
)

# COMMAND ----------

print("Silver row count:", silver_df.count())

# COMMAND ----------

print("Total rows:", silver_df.count())

print(
    "Distinct rows:",
    silver_df.distinct().count()
)

# COMMAND ----------

print(numeric_columns)

# COMMAND ----------

from pyspark.sql.functions import expr

silver_df = spark.table(
    "vehicle_analytics.fuel_data.bronze_vehicle_fuel"
)

for column_name, data_type in numeric_columns.items():
    silver_df = silver_df.withColumn(
        column_name,
        expr(f"try_cast(`{column_name}` AS {data_type})")
    )

silver_df.printSchema()

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "vehicle_analytics.fuel_data.silver_vehicle_fuel"
    )

# COMMAND ----------

spark.table(
    "vehicle_analytics.fuel_data.silver_vehicle_fuel"
).printSchema()