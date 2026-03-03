# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Utilities
# MAGIC Common functions used across notebooks

# COMMAND ----------

def get_table_stats(table_name: str) -> dict:
    """Get basic statistics for a Delta table."""
    df = spark.table(table_name)
    return {
        "table": table_name,
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns
    }

# COMMAND ----------

def validate_not_empty(df, name: str):
    """Raise error if DataFrame is empty."""
    if df.count() == 0:
        raise ValueError(f"{name} is empty!")
    return df

# COMMAND ----------

def log_metrics(stage: str, metrics: dict):
    """Log pipeline metrics."""
    import json
    print(f"[{stage}] {json.dumps(metrics)}")