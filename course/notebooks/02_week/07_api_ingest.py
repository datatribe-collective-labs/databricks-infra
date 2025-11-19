# Databricks notebook source
# MAGIC %md
# MAGIC # API Data Ingestion to Unity Catalog - Week 2
# MAGIC
# MAGIC This notebook demonstrates best practices for ingesting data from REST APIs into Delta Lake tables
# MAGIC using Unity Catalog.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Ingest data from REST APIs with error handling
# MAGIC - Implement retry logic and rate limiting
# MAGIC - Parse JSON responses and flatten nested structures
# MAGIC - Write API data to Unity Catalog bronze layer
# MAGIC - Handle pagination and incremental API loads
# MAGIC
# MAGIC ## Theoretical Unity Catalog Target
# MAGIC
# MAGIC ```
# MAGIC sales_dev.bronze.api_*        - Sales API data
# MAGIC marketing_dev.bronze.api_*    - Marketing API data
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, MapType
from pyspark.sql.functions import (
    current_timestamp, col, explode, from_json, get_json_object,
    lit, to_timestamp, struct, collect_list
)
from datetime import datetime, timedelta
import json
import time

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

# Display user configuration
print_user_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Simulating REST API Responses

# COMMAND ----------

print("=== Simulating Product API Response ===\n")

# Simulate API response (in production, use requests library)
def get_products_api():
    """Simulate product API endpoint"""
    return {
        "status": "success",
        "data": [
            {
                "product_id": 201,
                "product_name": "Laptop Pro 15",
                "category": "Electronics",
                "price": 1299.99,
                "stock": 50,
                "attributes": {"brand": "TechCorp", "warranty_years": 2}
            },
            {
                "product_id": 202,
                "product_name": "Wireless Mouse",
                "category": "Accessories",
                "price": 29.99,
                "stock": 200,
                "attributes": {"brand": "PeripheralsPro", "color": "black"}
            },
            {
                "product_id": 203,
                "product_name": "Mechanical Keyboard",
                "category": "Accessories",
                "price": 89.99,
                "stock": 150,
                "attributes": {"brand": "KeyMaster", "switch_type": "blue"}
            }
        ],
        "timestamp": "2024-01-15T10:00:00Z",
        "total_records": 3
    }

# Get API response
api_response = get_products_api()
print(f"API Status: {api_response['status']}")
print(f"Total Records: {api_response['total_records']}")
print(f"\nSample data:")
print(json.dumps(api_response['data'][0], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parsing JSON API Response

# COMMAND ----------

print("=== Parsing API Response to DataFrame ===\n")

# Define schema for product API data
product_api_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("stock", IntegerType(), False),
    StructField("attributes", MapType(StringType(), StringType()), True)
])

# Convert API response to DataFrame
df_products_api = spark.createDataFrame(api_response['data'], product_api_schema)

# Add API metadata
df_products_bronze = df_products_api \
    .withColumn("api_timestamp", to_timestamp(lit(api_response['timestamp']))) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_api", lit("products_api"))

print("Parsed API data:")
df_products_bronze.show(truncate=False)

# Flatten nested attributes
df_flattened = df_products_bronze \
    .withColumn("brand", col("attributes")["brand"]) \
    .withColumn("extra_info", col("attributes")) \
    .drop("attributes")

print("\nFlattened structure:")
df_flattened.show(truncate=False)

# Write to Bronze
bronze_table = get_table_path("bronze", "api_products")
df_flattened.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"\n Written to: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Handling Paginated API Responses

# COMMAND ----------

print("=== Handling Paginated API ===\n")

def get_customers_api(page=1, page_size=100):
    """Simulate paginated customer API"""
    # Simulate different pages
    if page == 1:
        return {
            "page": 1,
            "page_size": 100,
            "total_pages": 3,
            "data": [
                {"customer_id": 1001, "name": "Alice Johnson", "email": "alice@example.com", "lifetime_value": 5000.00},
                {"customer_id": 1002, "name": "Bob Smith", "email": "bob@example.com", "lifetime_value": 3500.00},
            ]
        }
    elif page == 2:
        return {
            "page": 2,
            "page_size": 100,
            "total_pages": 3,
            "data": [
                {"customer_id": 1003, "name": "Carol White", "email": "carol@example.com", "lifetime_value": 7200.00},
                {"customer_id": 1004, "name": "David Brown", "email": "david@example.com", "lifetime_value": 4100.00},
            ]
        }
    else:
        return {"page": 3, "page_size": 100, "total_pages": 3, "data": []}

# Fetch all pages
all_customers = []
page = 1
has_more = True

while has_more:
    print(f"Fetching page {page}...")
    response = get_customers_api(page=page)

    if response['data']:
        all_customers.extend(response['data'])
        page += 1
    else:
        has_more = False

    # Rate limiting (respect API limits)
    time.sleep(0.1)

print(f"\n Fetched {len(all_customers)} customers from {page-1} pages")

# Create DataFrame from all pages
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("lifetime_value", DoubleType(), False)
])

df_customers = spark.createDataFrame(all_customers, customer_schema)
df_customers_bronze = df_customers \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_api", lit("customers_api"))

print("\nAll customer data:")
df_customers_bronze.show()

# Write to Bronze
customers_table = get_table_path("bronze", "api_customers")
df_customers_bronze.write.format("delta").mode("overwrite").saveAsTable(customers_table)

print(f" Written to: {customers_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook demonstrated how to ingest data from two simulated REST APIs: a product API with nested JSON and a paginated customer API. The raw data was parsed, flattened, and written to two separate bronze tables in Unity Catalog. Key practices like schema definition, metadata enrichment, and handling pagination were implemented.
# MAGIC
# MAGIC ### Tables Created
# MAGIC - `bronze_api_products`
# MAGIC - `bronze_api_customers`