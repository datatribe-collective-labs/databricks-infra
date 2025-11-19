# Databricks notebook source
# MAGIC %md
# MAGIC # Database Ingestion (JDBC) - Week 2
# MAGIC
# MAGIC Learn to ingest data from relational databases into Unity Catalog using JDBC.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

print_user_config()

# Simulate database table data
customers_db = [
    (1, "Alice", "alice@example.com", "New York"),
    (2, "Bob", "bob@example.com", "San Francisco"),
    (3, "Carol", "carol@example.com", "Chicago")
]

df = spark.createDataFrame(customers_db, ["id", "name", "email", "city"])
df_bronze = df.withColumn("ingestion_timestamp", current_timestamp())

# Write to Unity Catalog
table = get_table_path("bronze", "db_customers")
df_bronze.write.format("delta").mode("overwrite").saveAsTable(table)

print(f"✅ Written to: {table}")
spark.table(table).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated a simplified database ingestion workflow. It simulated a customer database table, loaded it into a DataFrame, enriched it with an ingestion timestamp, and wrote the result to a bronze Delta table in Unity Catalog, preparing it for downstream processing.
# MAGIC
# MAGIC ### Table Created
# MAGIC - `bronze_db_customers`
