# Databricks notebook source
# MAGIC %md
# MAGIC # Window Transformations - Week 3
# MAGIC  
# MAGIC Use window functions for ranking, running totals, and analytics.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum as _sum, avg, lag, lead, col

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

# Read from reference catalog or user's own silver table
df_sales = spark.table("sales_dev.silver.sales_transactions")
# Alternative: Read from user's own table created in previous notebook
# df_sales = spark.table(get_table_path("silver", "sales_transactions"))

# Window by store_location, ordered by total_amount
window_spec = Window.partitionBy("store_location").orderBy(col("total_amount").desc())

# Apply window functions
df_windowed = df_sales \
    .withColumn("rank_in_store", rank().over(window_spec)) \
    .withColumn("row_num_in_store", row_number().over(window_spec))

print("Window transformations (rank by store):")
df_windowed.select("transaction_id", "store_location", "total_amount", "rank_in_store").show()

# Running total by date
date_window = Window.partitionBy("store_location").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_running = df_sales \
    .withColumn("running_total", _sum("total_amount").over(date_window))

print("\nRunning totals:")
df_running.select("store_location", "transaction_date", "total_amount", "running_total").show()

# Write results to user's silver layer
result_table = get_table_path("silver", "sales_with_analytics")
df_windowed.write.format("delta").mode("overwrite").saveAsTable(result_table)

print(f"✅ Written to: {result_table}")
