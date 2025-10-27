# Databricks notebook source
# MAGIC %md
# MAGIC # S3/Cloud Storage Ingestion - Week 2
# MAGIC  
# MAGIC Learn to ingest data from cloud storage into Unity Catalog.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

print_user_config()

# Simulate S3 file data
s3_data = [
    ("file1.csv", "2024-01-15", 1000),
    ("file2.csv", "2024-01-16", 1500),
    ("file3.csv", "2024-01-17", 2000)
]

df = spark.createDataFrame(s3_data, ["filename", "date", "record_count"])
df_bronze = df.withColumn("ingestion_timestamp", current_timestamp())

# Write to Unity Catalog  
table = get_table_path("bronze", "s3_files")
df_bronze.write.format("delta").mode("overwrite").saveAsTable(table)

print(f"✅ Written to: {table}")
spark.table(table).show()
