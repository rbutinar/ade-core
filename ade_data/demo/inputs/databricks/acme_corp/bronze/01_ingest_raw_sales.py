# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Raw Sales Data
# MAGIC Load daily sales transactions from source system

# COMMAND ----------

from pyspark.sql import functions as F

# Configuration
SOURCE_PATH = "/mnt/raw/sales/"
TARGET_TABLE = "bronze.raw_sales"

# COMMAND ----------

# Read raw CSV files
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(SOURCE_PATH)

print(f"Loaded {df_raw.count()} records")

# COMMAND ----------

# Add ingestion metadata
df_with_meta = df_raw \
    .withColumn("_ingested_at", F.current_timestamp()) \
    .withColumn("_source_file", F.input_file_name())

# COMMAND ----------

# Write to bronze table
df_with_meta.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TARGET_TABLE)

print(f"Written to {TARGET_TABLE}")