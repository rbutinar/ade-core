# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregate Sales
# MAGIC Create daily and monthly sales aggregations for reporting

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "silver.clean_sales"
DAILY_TABLE = "gold.daily_sales"
MONTHLY_TABLE = "gold.monthly_sales"

# COMMAND ----------

df = spark.table(SOURCE_TABLE)

# COMMAND ----------

# Daily aggregation
df_daily = df.groupBy("sale_date", "product_category") \
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("unique_customers")
    )

df_daily.write.format("delta").mode("overwrite").saveAsTable(DAILY_TABLE)

# COMMAND ----------

# Monthly aggregation
df_monthly = df \
    .withColumn("sale_month", F.date_trunc("month", "sale_date")) \
    .groupBy("sale_month", "product_category") \
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("unique_customers")
    )

df_monthly.write.format("delta").mode("overwrite").saveAsTable(MONTHLY_TABLE)

print("Aggregations complete")