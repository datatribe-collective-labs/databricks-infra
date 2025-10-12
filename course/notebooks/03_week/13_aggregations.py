# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregations - Silver to Gold - Week 3
# MAGIC
# MAGIC Create business-level aggregations for Gold layer.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, count, max as _max, min as _min, to_date

CATALOG = "sales_dev"
SILVER = "silver"
GOLD = "gold"

# Read silver data
df_silver = spark.table(f"{CATALOG}.{SILVER}.sales_transactions")

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
df_products = spark.table(f"{CATALOG}.bronze.product_inventory")

df_category_sales = df_silver \
    .join(df_products, "product_id") \
    .groupBy("category") \
    .agg(
        _sum("total_amount").alias("category_revenue"),
        count("transaction_id").alias("category_transactions")
    )

print("\nCategory aggregations:")
df_category_sales.show()

# Write to Gold
gold_table = f"{CATALOG}.{GOLD}.daily_sales_summary"
df_daily.write.format("delta").mode("overwrite").saveAsTable(gold_table)

gold_category_table = f"{CATALOG}.{GOLD}.category_performance"
df_category_sales.write.format("delta").mode("overwrite").saveAsTable(gold_category_table)

print(f"✅ Written to Gold: {gold_table}, {gold_category_table}")
