# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Transformations - Bronze to Silver - Week 3
# MAGIC
# MAGIC Transform raw sales data from Bronze to clean Silver layer.

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, when, regexp_replace
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

# Read from reference catalog (shared, read-only) OR user's own bronze table
# Option 1: Read from shared reference catalog
df_bronze = spark.table("sales_dev.bronze.sales_transactions")

# Option 2: Read from user's own bronze table (if they created it in previous notebooks)
# df_bronze = spark.table(get_table_path("bronze", "sales_transactions"))

print("Bronze data (raw):")
df_bronze.show(5)

# COMMAND ----------

# Apply cleaning transformations
df_silver = df_bronze \
    .withColumn("store_location", trim(upper(col("store_location")))) \
    .withColumn("is_valid_transaction",
        when((col("quantity") > 0) & (col("total_amount") > 0), True).otherwise(False)
    ) \
    .filter(col("is_valid_transaction") == True) \
    .drop("is_valid_transaction")

print("\nSilver data (cleaned):")
df_silver.show(5)

# Write to user's Silver layer
silver_table = get_table_path("silver", "sales_transactions")
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"✅ Written to: {silver_table}")
