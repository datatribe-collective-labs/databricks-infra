# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End: API to Aggregation - Week 4
# MAGIC
# MAGIC Complete pipeline from API ingestion to business metrics.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

# Step 1: Simulate API ingestion
print("=== Step 1: API Ingestion (Bronze) ===")
api_data = [
    ("camp_004", "Spring Campaign", 15000, 180000, 9500, 750, 82500),
    ("camp_005", "Summer Sale", 12000, 150000, 8200, 650, 71500),
]
df_api = spark.createDataFrame(api_data, ["campaign_id", "name", "budget", "impressions", "clicks", "conversions", "revenue"])
df_api = df_api.withColumn("ingestion_timestamp", current_timestamp())

bronze_table = get_table_path("bronze", "campaign_data")
df_api.write.format("delta").mode("append").saveAsTable(bronze_table)
print(f"✅ Bronze: {bronze_table}")

# Step 2: Calculate metrics (Silver)
print("\n=== Step 2: Calculate Metrics (Silver) ===")
df_bronze = spark.table(bronze_table)
df_silver = df_bronze \
    .withColumn("ctr", round(col("clicks") / col("impressions") * 100, 2)) \
    .withColumn("conversion_rate", round(col("conversions") / col("clicks") * 100, 2)) \
    .withColumn("roas", round(col("revenue") / col("budget"), 2))

silver_table = get_table_path("silver", "campaign_metrics")
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)
print(f"✅ Silver: {silver_table}")

# Step 3: Aggregate performance (Gold)
print("\n=== Step 3: Performance Summary (Gold) ===")
df_gold = df_silver \
    .agg(
        sum("budget").alias("total_budget"),
        sum("revenue").alias("total_revenue"),
        avg("roas").alias("avg_roas"),
        sum("conversions").alias("total_conversions")
    )

gold_table = get_table_path("gold", "campaign_performance")
df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)
print(f"✅ Gold: {gold_table}")

df_gold.show()
