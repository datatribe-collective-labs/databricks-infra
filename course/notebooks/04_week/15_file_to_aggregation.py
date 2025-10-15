# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End: File Ingestion to Aggregation - Week 4
# MAGIC
# MAGIC Complete Bronze → Silver → Gold pipeline.

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

CATALOG = "sales_dev"

# Step 1: Bronze - Ingest raw data
print("=== Step 1: Bronze Layer ===")
raw_data = [
    (6001, 501, 601, "Tablet", 2, 499.99, 999.98, "2024-01-20", "Miami"),
    (6002, 502, 602, "Case", 5, 19.99, 99.95, "2024-01-20", "Miami"),
]
df_raw = spark.createDataFrame(raw_data, ["transaction_id", "customer_id", "product_id", "product_name", "quantity", "unit_price", "total_amount", "transaction_date", "store_location"])
df_raw = df_raw.withColumn("ingestion_timestamp", current_timestamp())

bronze_table = f"{CATALOG}.bronze.daily_sales"
df_raw.write.format("delta").mode("append").saveAsTable(bronze_table)
print(f"✅ Bronze: {bronze_table}")

# Step 2: Silver - Clean and validate
print("\n=== Step 2: Silver Layer ===")
df_bronze = spark.table(bronze_table)
df_silver = df_bronze \
    .filter(col("quantity") > 0) \
    .withColumn("store_location", upper(col("store_location")))

silver_table = f"{CATALOG}.silver.daily_sales"
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)
print(f"✅ Silver: {silver_table}")

# Step 3: Gold - Aggregate
print("\n=== Step 3: Gold Layer ===")
df_gold = df_silver \
    .groupBy(to_date("transaction_date").alias("date"), "store_location") \
    .agg(
        sum("total_amount").alias("revenue"),
        count("*").alias("transactions")
    )

gold_table = f"{CATALOG}.gold.daily_summary"
df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)
print(f"✅ Gold: {gold_table}")

df_gold.show()
