# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Sales Data
# MAGIC Transform raw sales into clean, validated records

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

SOURCE_TABLE = "bronze.raw_sales"
TARGET_TABLE = "silver.clean_sales"

# COMMAND ----------

# Read from bronze
df = spark.table(SOURCE_TABLE)

# COMMAND ----------

# Data quality transformations
df_clean = df \
    .filter(F.col("amount").isNotNull()) \
    .filter(F.col("amount") > 0) \
    .withColumn("sale_date", F.to_date("sale_date")) \
    .withColumn("amount", F.col("amount").cast("decimal(18,2)")) \
    .withColumn("customer_id", F.upper(F.trim("customer_id"))) \
    .dropDuplicates(["transaction_id"])

# COMMAND ----------

# Write to silver
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"Clean records: {df_clean.count()}")