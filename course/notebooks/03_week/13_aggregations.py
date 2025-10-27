# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregations - Silver to Gold - Week 3
# MAGIC
# MAGIC Create business-level aggregations for Gold layer.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, count, max as _max, min as _min, to_date

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup

# COMMAND ----------

# Read from reference catalog or user's own silver table
df_silver = spark.table("sales_dev.silver.sales_transactions")
# Alternative: Read from user's own table
# df_silver = spark.table(get_table_path("silver", "sales_transactions"))

# Daily aggregations
df_daily = df_silver \
    .groupBy(to_date("transaction_date").alias("date"), "store_location") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        _sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_transaction_value"),
        _max("total_amount").alias("max_transaction"),
        _min("total_amount").alias("min_transaction")
    )

print("Daily aggregations:")
df_daily.show()

# Product category aggregations
df_products = spark.table("sales_dev.bronze.product_inventory")

df_category_sales = df_silver \
    .join(df_products, "product_id") \
    .groupBy("category") \
    .agg(
        _sum("total_amount").alias("category_revenue"),
        count("transaction_id").alias("category_transactions")
    )

print("\nCategory aggregations:")
df_category_sales.show()

# Write to user's Gold layer
gold_table = get_table_path("gold", "daily_sales_summary")
df_daily.write.format("delta").mode("overwrite").saveAsTable(gold_table)

gold_category_table = get_table_path("gold", "category_performance")
df_category_sales.write.format("delta").mode("overwrite").saveAsTable(gold_category_table)

print(f"✅ Written to Gold: {gold_table}, {gold_category_table}")
