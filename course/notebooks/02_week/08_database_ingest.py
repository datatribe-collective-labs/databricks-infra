# Databricks notebook source
# MAGIC %md
# MAGIC # Database Ingestion (JDBC) - Week 2
# MAGIC
# MAGIC Learn to ingest data from relational databases into Unity Catalog using JDBC.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

CATALOG = "sales_dev"
BRONZE_SCHEMA = "bronze"

# Simulate database table data
customers_db = [
    (1, "Alice", "alice@example.com", "New York"),
    (2, "Bob", "bob@example.com", "San Francisco"),
    (3, "Carol", "carol@example.com", "Chicago")
]

df = spark.createDataFrame(customers_db, ["id", "name", "email", "city"])
df_bronze = df.withColumn("ingestion_timestamp", current_timestamp())

# Write to Unity Catalog
table = f"{CATALOG}.{BRONZE_SCHEMA}.db_customers"
df_bronze.write.format("delta").mode("overwrite").saveAsTable(table)

print(f"✅ Written to: {table}")
spark.table(table).show()
