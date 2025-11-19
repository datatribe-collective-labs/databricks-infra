# Databricks notebook source
# MAGIC %md
# MAGIC # File Ingestion to Unity Catalog - Week 2
# MAGIC
# MAGIC This notebook demonstrates best practices for ingesting files (CSV, JSON, Parquet) into Delta Lake tables
# MAGIC using Unity Catalog's three-level namespace.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Ingest CSV, JSON, and Parquet files with explicit schemas
# MAGIC - Write data to Unity Catalog (bronze layer)
# MAGIC - Implement error handling and data quality checks
# MAGIC - Use partition strategies for performance
# MAGIC
# MAGIC ## Theoritical Example Unity Catalog Structure
# MAGIC
# MAGIC ```
# MAGIC sales_dev.bronze.*      - Raw sales data (development)
# MAGIC sales_prod.bronze.*     - Raw sales data (production)
# MAGIC marketing_dev.bronze.*  - Raw marketing data (development)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, col, lit
from datetime import date, datetime

print(f"Target Catalog: {CATALOG}")
print(f"Bronze Schema: {CATALOG}.{USER_SCHEMA}")

# COMMAND ----------

# Display user configuration
print_user_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CSV File Ingestion with Explicit Schema

# COMMAND ----------

print("=== CSV File Ingestion ===\n")

# Define explicit schema for sales transactions
sales_schema = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("store_location", StringType(), nullable=True)
])

# Sample CSV data (simulating file from source system)
sales_data = [
    (1001, 101, 201, "Laptop", 1, 1299.99, 1299.99, date(2024, 1, 15), "New York"),
    (1002, 102, 202, "Mouse", 2, 29.99, 59.98, date(2024, 1, 15), "San Francisco"),
    (1003, 103, 203, "Keyboard", 1, 89.99, 89.99, date(2024, 1, 15), "Chicago"),
    (1004, 101, 204, "Monitor", 1, 399.99, 399.99, date(2024, 1, 16), "New York"),
    (1005, 104, 201, "Laptop", 1, 1299.99, 1299.99, date(2024, 1, 16), "Seattle"),
]

df_sales = spark.createDataFrame(sales_data, sales_schema)

# Add metadata columns for lineage
df_sales_bronze = df_sales \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("sales_transactions_2024_01.csv"))

print("Sample sales data:")
df_sales_bronze.show(5, truncate=False)

# Write to Bronze layer in Unity Catalog
bronze_table = get_table_path("bronze", "sales_transactions")

df_sales_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_table)

print(f"✅ Written to: {bronze_table}")

# COMMAND ----------

# Verify the data
print("=== Verifying Bronze Table ===\n")

df_verify = spark.table(bronze_table)
print(f"Record count: {df_verify.count()}")
print(f"\nSchema:")
df_verify.printSchema()

print("\nSample records:")
df_verify.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. JSON File Ingestion with Schema Validation

# COMMAND ----------

print("=== JSON File Ingestion ===\n")

# Define schema for customer events
customer_events_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("page_url", StringType(), nullable=True),
    StructField("session_id", StringType(), nullable=True),
    StructField("user_agent", StringType(), nullable=True)
])

# Sample JSON data
events_data = [
    ("evt_001", 101, "page_view", datetime(2024, 1, 15, 10, 30, 0), "/products/laptop", "sess_abc123", "Mozilla/5.0"),
    ("evt_002", 101, "add_to_cart", datetime(2024, 1, 15, 10, 35, 0), "/cart", "sess_abc123", "Mozilla/5.0"),
    ("evt_003", 102, "page_view", datetime(2024, 1, 15, 11, 0, 0), "/products/mouse", "sess_xyz789", "Chrome/120.0"),
    ("evt_004", 101, "purchase", datetime(2024, 1, 15, 10, 40, 0), "/checkout/success", "sess_abc123", "Mozilla/5.0"),
    ("evt_005", 103, "page_view", datetime(2024, 1, 15, 12, 0, 0), "/products/keyboard", "sess_def456", "Safari/17.0"),
]

df_events = spark.createDataFrame(events_data, customer_events_schema)

# Add metadata
df_events_bronze = df_events \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("web_analytics"))

print("Sample customer events:")
df_events_bronze.show(5, truncate=False)

# Write to Bronze layer
bronze_events_table = get_table_path("bronze", "customer_events")

df_events_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(bronze_events_table)

print(f"✅ Written to: {bronze_events_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parquet File Ingestion with Partitioning

# COMMAND ----------

print("=== Parquet File Ingestion with Partitioning ===\n")

# Define schema for product inventory
inventory_schema = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("stock_quantity", IntegerType(), nullable=False),
    StructField("warehouse_location", StringType(), nullable=False),
    StructField("last_updated", TimestampType(), nullable=False),
    StructField("snapshot_date", DateType(), nullable=False)
])

# Sample inventory data
inventory_data = [
    (201, "Laptop", "Electronics", 50, "Warehouse_A", datetime(2024, 1, 15, 8, 0, 0), date(2024, 1, 15)),
    (202, "Mouse", "Accessories", 200, "Warehouse_A", datetime(2024, 1, 15, 8, 0, 0), date(2024, 1, 15)),
    (203, "Keyboard", "Accessories", 150, "Warehouse_B", datetime(2024, 1, 15, 8, 0, 0), date(2024, 1, 15)),
    (204, "Monitor", "Electronics", 75, "Warehouse_A", datetime(2024, 1, 15, 8, 0, 0), date(2024, 1, 15)),
    (205, "Desk", "Furniture", 30, "Warehouse_C", datetime(2024, 1, 15, 8, 0, 0), date(2024, 1, 15)),
]

df_inventory = spark.createDataFrame(inventory_data, inventory_schema)

# Add metadata
df_inventory_bronze = df_inventory \
    .withColumn("ingestion_timestamp", current_timestamp())

print("Sample inventory data:")
df_inventory_bronze.show(5, truncate=False)

# Write to Bronze layer with partitioning
bronze_inventory_table = get_table_path("bronze", "product_inventory")

df_inventory_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("snapshot_date") \
    .saveAsTable(bronze_inventory_table)

print(f"✅ Written to: {bronze_inventory_table}")
print(f"✅ Partitioned by: snapshot_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Error Handling and Data Quality

# COMMAND ----------

print("=== Data Quality Validation ===\n")

"""
This was defined before with customer_id field nullable=False. Test run it to see differences in execution.
    StructField("customer_id", IntegerType(), nullable=True),
"""
sales_schema = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("total_amount", DoubleType(), nullable=False),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("store_location", StringType(), nullable=True)
])


"""
UNCOMMENT code blocks to test differences in Version 1 and 2.
NOTICE: Date format, negative and null values.
"""
# Version 1: Create data with quality issues. 
""" 
sales_with_issues = [
    (2001, 201, 301, "Tablet", 1, 599.99, 599.99, "2024-01-17", "Boston"),
    (2002, 202, 302, "Charger", -5, 19.99, -99.95, "2024-01-17", "Boston"),  # Negative quantity
    (2003, 203, 303, "Cable", 3, 9.99, 29.97, "2024-01-17", "Boston"),
    (2004, None, 304, "Adapter", 2, 14.99, 29.98, "2024-01-17", "Boston"),  # Null customer_id
] 
"""

# Version 2: Create data with quality issues
sales_with_issues = [
    (2001, 201, 301, "Tablet", 1, 599.99, 599.99, date(2024, 1, 7), "Boston"),
    (2002, 202, 302, "Charger", -5, 19.99, -99.95, date(2024, 1, 17), "Boston"),  # Negative quantity
    (2003, 203, 303, "Cable", 3, 9.99, 29.97, date(2024, 1, 17), "Boston"),
    (2004, None, 304, "Adapter", 2, 14.99, 29.98, date(2024, 1, 17), "Boston"),  # customer_id is null
]

df_sales_issues = spark.createDataFrame(sales_with_issues, sales_schema)

# Add data quality flags
df_with_quality = df_sales_issues \
    .withColumn("is_valid",
        col("customer_id").isNotNull() &
        (col("quantity") > 0) &
        (col("total_amount") > 0)
    ) \
    .withColumn("ingestion_timestamp", current_timestamp())

print("Data with quality flags:")
df_with_quality.show(truncate=False)

# Separate good and bad data
df_good = df_with_quality.filter(col("is_valid") == True)
df_bad = df_with_quality.filter(col("is_valid") == False)

print(f"\n✅ Good records: {df_good.count()}")
print(f"❌ Bad records: {df_bad.count()}")

# Write good data to Bronze
good_table = get_table_path("bronze", "sales_validated")
df_good.write.format("delta").mode("overwrite").saveAsTable(good_table)

# Write bad data to quarantine
quarantine_table = get_table_path("bronze", "sales_quarantine")
df_bad.write.format("delta").mode("overwrite").saveAsTable(quarantine_table)

print(f"\n✅ Good data → {good_table}")
print(f"⚠️ Bad data → {quarantine_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Incremental File Ingestion

# COMMAND ----------

print("=== Incremental File Ingestion ===\n")

# Initial load
initial_sales = [
    (3001, 301, 401, "Headphones", 1, 149.99, 149.99, date(2024, 1, 18), "Austin"),
    (3002, 302, 402, "Speaker", 2, 79.99, 159.98, date(2024, 1, 18), "Austin"),
]

df_initial = spark.createDataFrame(initial_sales, sales_schema)

incremental_table = get_table_path("bronze", "sales_incremental")

# Initial write
df_initial.write.format("delta").mode("overwrite").saveAsTable(incremental_table)
print(f"Initial load: {df_initial.count()} records")

# Incremental load (new data arrives)
new_sales = [
    (3003, 303, 403, "Webcam", 1, 89.99, 89.99, date(2024, 1, 18), "Austin"),
    (3004, 304, 404, "Microphone", 1, 129.99, 129.99, date(2024, 1, 18), "Denver"),
]

df_new = spark.createDataFrame(new_sales, sales_schema)

# Append new data
df_new.write.format("delta").mode("append").saveAsTable(incremental_table)
print(f"Incremental load: {df_new.count()} records added")

# Verify total
df_total = spark.table(incremental_table)
print(f"Total records: {df_total.count()}")

df_total.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cross-Catalog Ingestion (Marketing Data)

# COMMAND ----------

print("=== Marketing Campaign Data Ingestion ===\n")

# Define schema for marketing campaigns
campaign_schema = StructType([
    StructField("campaign_id", StringType(), nullable=False),
    StructField("campaign_name", StringType(), nullable=False),
    StructField("channel", StringType(), nullable=False),
    StructField("budget", DoubleType(), nullable=False),
    StructField("start_date", DateType(), nullable=False),
    StructField("end_date", DateType(), nullable=False),
    StructField("target_audience", StringType(), nullable=True)
])

# Sample marketing data
campaign_data = [
    ("camp_001", "Winter Sale 2024", "email", 5000.00, date(2024, 1, 15), date(2024, 1, 31), "existing_customers"),
    ("camp_002", "New Product Launch", "social_media", 10000.00, date(2024, 1, 20), date(2024, 2, 15), "tech_enthusiasts"),
    ("camp_003", "Seasonal Promotion", "display_ads", 7500.00, date(2024, 1, 25), date(2024, 2, 10), "all_users"),
]

df_campaigns = spark.createDataFrame(campaign_data, campaign_schema)

# Add metadata
df_campaigns_bronze = df_campaigns \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("marketing_platform"))

print("Marketing campaign data:")
df_campaigns_bronze.show(truncate=False)

# Write to user's schema
marketing_table = get_table_path("bronze", "campaigns")

df_campaigns_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(marketing_table)

print(f"✅ Written to: {marketing_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC 1. **Explicit Schemas**: Always define schemas for production data
# MAGIC 2. **Unity Catalog**: Use three-level namespace (catalog.schema.table)
# MAGIC 3. **Bronze Layer**: Store raw data with metadata (ingestion_timestamp, source)
# MAGIC 4. **Partitioning**: Use for large datasets (improves query performance)
# MAGIC 5. **Data Quality**: Validate and quarantine bad data
# MAGIC 6. **Incremental Loading**: Append new data efficiently
# MAGIC
# MAGIC ### Tables Created
# MAGIC
# MAGIC All tables are created within the `databricks_course` catalog and a user-specific schema, following the pattern:
# MAGIC `databricks_course.<your_schema_name>.<table_name>`.
# MAGIC
# MAGIC **Bronze Layer Tables Created:**
# MAGIC - `bronze_sales_transactions` (CSV ingestion)
# MAGIC - `bronze_customer_events` (JSON ingestion)
# MAGIC - `bronze_product_inventory` (Parquet with partitioning)
# MAGIC - `bronze_sales_validated` (quality-checked data)
# MAGIC - `bronze_sales_quarantine` (rejected records)
# MAGIC - `bronze_sales_incremental` (incremental loads)
# MAGIC - `bronze_campaigns` (marketing data)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Move on API ingestion in notebook 7
# MAGIC - Learn about database ingestion (JDBC) in notebook 8
# MAGIC - Explore S3/cloud storage ingestion in notebook 9
