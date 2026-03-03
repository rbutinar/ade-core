# Databricks notebook source
# MAGIC %md
# MAGIC # Enrich Customer Data
# MAGIC Join customer demographics with transaction history

# COMMAND ----------

from pyspark.sql import functions as F

CUSTOMERS_TABLE = "bronze.raw_customers"
SALES_TABLE = "silver.clean_sales"
TARGET_TABLE = "silver.enriched_customers"

# COMMAND ----------

df_customers = spark.table(CUSTOMERS_TABLE)
df_sales = spark.table(SALES_TABLE)

# COMMAND ----------

# Calculate customer metrics
df_metrics = df_sales.groupBy("customer_id") \
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("lifetime_value"),
        F.min("sale_date").alias("first_purchase"),
        F.max("sale_date").alias("last_purchase")
    )

# COMMAND ----------

# Join with customer demographics
df_enriched = df_customers \
    .join(df_metrics, "customer_id", "left") \
    .withColumn("customer_segment", 
        F.when(F.col("lifetime_value") > 10000, "Premium")
         .when(F.col("lifetime_value") > 1000, "Regular")
         .otherwise("New"))

df_enriched.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)