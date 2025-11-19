# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion Concepts Explained - Week 2
# MAGIC
# MAGIC This notebook provides a practical exploration of data ingestion patterns, focusing on
# MAGIC understanding core concepts through hands-on examples.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand batch vs streaming ingestion
# MAGIC - Learn schema handling strategies (inference vs explicit)
# MAGIC - Master error handling and data quality patterns
# MAGIC - Explore idempotent ingestion patterns
# MAGIC - Understand incremental loading strategies
# MAGIC
# MAGIC ## Topics Covered
# MAGIC
# MAGIC 1. Batch vs Streaming: When to use what
# MAGIC 2. Schema Strategies: Inference vs Explicit
# MAGIC 3. Error Handling Patterns
# MAGIC 4. Idempotent Ingestion
# MAGIC 5. Incremental Loading Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Batch vs Streaming: Understanding the Difference
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Batch Ingestion:**
# MAGIC - Processes data in large chunks at scheduled intervals
# MAGIC - Complete dataset available before processing
# MAGIC - Optimized for throughput, not latency
# MAGIC
# MAGIC **Streaming Ingestion:**
# MAGIC - Processes data continuously as it arrives
# MAGIC - Records processed individually or in micro-batches
# MAGIC - Optimized for low latency, near real-time
# MAGIC
# MAGIC ### When to Use What?
# MAGIC
# MAGIC | Use Case | Batch | Streaming |
# MAGIC |----------|-------|-----------|
# MAGIC | Daily reports | ✅ | ❌ |
# MAGIC | Real-time dashboards | ❌ | ✅ |
# MAGIC | Monthly aggregations | ✅ | ❌ |
# MAGIC | Fraud detection | ❌ | ✅ |
# MAGIC | ETL pipelines | ✅ | ❌ |
# MAGIC | Event processing | ❌ | ✅ |
# MAGIC | Historical data load | ✅ | ❌ |
# MAGIC | IoT sensor data | ❌ | ✅ |

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, col, lit, from_json, to_json, struct
from datetime import datetime, timedelta
import time

print("=== Batch Ingestion Example ===\n")

# Simulate daily sales file arriving
sales_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_date", TimestampType(), False)
])

# Day 1 batch
day1_sales = [
    (1001, 101, 501, 129.99, datetime(2024, 1, 15, 10, 30)),
    (1002, 102, 502, 89.50, datetime(2024, 1, 15, 11, 45)),
    (1003, 103, 503, 199.99, datetime(2024, 1, 15, 14, 20)),
    (1004, 101, 504, 129.99, datetime(2024, 1, 15, 16, 10))
]

df_batch = spark.createDataFrame(day1_sales, sales_schema)

print("Batch ingestion - Process entire day's data at once:")
print(f"Records in batch: {df_batch.count()}")
df_batch.show(truncate=False)

# Write to Delta (batch mode) - Using Unity Catalog
batch_table = get_table_path("bronze", "batch_sales")
df_batch.write.format("delta").mode("overwrite").saveAsTable(batch_table)

print(f"✅ Batch written to: {batch_table}")
print("   Characteristics: High throughput, scheduled processing, complete dataset")

# COMMAND ----------

print("=== Streaming Ingestion Example ===\n")

# Simulate streaming transactions (using memory source for demo)
streaming_sales = [
    {"transaction_id": 2001, "product_id": 101, "customer_id": 601, "amount": 149.99, "timestamp": datetime.now().isoformat()},
    {"transaction_id": 2002, "product_id": 102, "customer_id": 602, "amount": 99.50, "timestamp": (datetime.now() + timedelta(seconds=1)).isoformat()},
    {"transaction_id": 2003, "product_id": 103, "customer_id": 603, "amount": 299.99, "timestamp": (datetime.now() + timedelta(seconds=2)).isoformat()}
]

# Create a streaming DataFrame (simulated with rate source)
streaming_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .withColumn("transaction_id", (col("value") + 3000).cast("int")) \
    .withColumn("product_id", ((col("value") % 3) + 101).cast("int")) \
    .withColumn("customer_id", ((col("value") % 5) + 701).cast("int")) \
    .withColumn("amount", (col("value") % 100 + 50).cast("double")) \
    .select("transaction_id", "product_id", "customer_id", "amount", "timestamp")

print("Streaming ingestion setup:")
print(f"  Source: rate (1 record/second)")
print(f"  Processing: Continuous micro-batches")
print(f"  Schema: {streaming_df.schema.simpleString()}")

"""
# Note: In a real notebook, you would run this streaming query
 streaming_query = streaming_df.writeStream \
     .format("delta") \
     .outputMode("append") \
     .option("checkpointLocation", "/tmp/ingestion_examples/checkpoint") \
     .start("/tmp/ingestion_examples/streaming_sales")
"""

print("\n✅ Streaming concept demonstrated")
print("   Characteristics: Low latency, continuous processing, record-by-record")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema Strategies: Inference vs Explicit
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Schema Inference:**
# MAGIC - Spark reads sample of data to determine types
# MAGIC - Convenient for exploration and prototyping
# MAGIC - ⚠️ Risky for production (type mismatches, performance overhead)
# MAGIC
# MAGIC **Explicit Schema:**
# MAGIC - Define schema upfront in code
# MAGIC - Guarantees data types and structure
# MAGIC - ✅ Recommended for production (fast, reliable, type-safe)
# MAGIC
# MAGIC ### Why Explicit Schemas Matter
# MAGIC
# MAGIC 1. **Performance**: No scanning required to infer types
# MAGIC 2. **Reliability**: Fails fast on schema mismatches
# MAGIC 3. **Data Quality**: Enforces expected structure
# MAGIC 4. **Documentation**: Schema serves as contract

# COMMAND ----------

import json

print("=== Schema Inference (Convenient but Risky) ===\n")

# Create sample JSON data
sample_json_data = [
    {"user_id": "1", "age": "25", "score": "95.5", "active": "true"},  # All strings!
    {"user_id": "2", "age": "30", "score": "87.3", "active": "false"},
    {"user_id": "3", "age": "invalid", "score": "92.1", "active": "true"}  # Bad data
]

# Write JSON data to Delta table (instead of temp file)
json_strings = [json.dumps(record) for record in sample_json_data]
from pyspark.sql import Row
df_json_strings = spark.createDataFrame([Row(value=js) for js in json_strings])

temp_infer_table = get_table_path("bronze", "temp_schema_inference_data")
df_json_strings.write.format("delta").mode("overwrite").saveAsTable(temp_infer_table)

print("Reading with schema inference:")

# Define schema for JSON
json_schema = StructType([
    StructField("user_id", StringType()),
    StructField("age", StringType()),
    StructField("score", StringType()),
    StructField("active", StringType())
])

# Read Delta table
df_json_strings = spark.table(temp_infer_table)

# Parse JSON strings using from_json
df_inferred = df_json_strings.withColumn(
    "parsed",
    from_json(col("value"), json_schema)
).select("parsed.*")

print("\nInferred schema:")
df_inferred.printSchema()

print("\nData with inferred types:")
df_inferred.show()

print("\n⚠️ Problems with inference:")
print("  - All fields became strings (not ideal)")
print("  - 'invalid' age accepted as string")
print("  - No type validation")
print("  - Performance overhead from scanning")

# COMMAND ----------

print("=== Explicit Schema (Production-Ready) ===\n")


# Define explicit schema
user_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("age", IntegerType(), False),
    StructField("score", DoubleType(), False),
    StructField("active", StringType(), False)  # We'll validate boolean separately
])

print("Defined explicit schema:")
for field in user_schema.fields:
    nullable = "nullable" if field.nullable else "required"
    print(f"  - {field.name}: {field.dataType.simpleString()} ({nullable})")

# Read with explicit schema (from the same data that had bad values)
print("\nReading with explicit schema:")
try:
    # To demonstrate FAILFAST, we write the JSON strings to a temporary path 
    # and read it back with the FAILFAST option.
    temp_json_path = f"{VOLUME_PATH}temp_json_for_failfast"
    spark.table(temp_infer_table).select("value").write.mode("overwrite").text(temp_json_path)
    
    df_explicit = spark.read.schema(user_schema).option("mode", "FAILFAST").json(temp_json_path)
    df_explicit.show()
except Exception as e:
    print(f"✅ Schema validation caught error: {str(e)}")
    print("   This is GOOD - fails fast on bad data!")

# Clean data example
clean_json_data = [
    {"user_id": 1, "age": 25, "score": 95.5, "active": "true"},
    {"user_id": 2, "age": 30, "score": 87.3, "active": "false"},
    {"user_id": 3, "age": 28, "score": 92.1, "active": "true"}
]

# Write clean JSON data to Delta table
json_strings_clean = [json.dumps(record) for record in clean_json_data]
df_json_clean = spark.createDataFrame([Row(value=js) for js in json_strings_clean])

temp_clean_table = get_table_path("bronze", "temp_schema_clean_data")
df_json_clean.write.format("delta").mode("overwrite").saveAsTable(temp_clean_table)


# Read clean data with explicit schema using from_json
df_json_clean_read = spark.table(temp_clean_table).select("value")
df_explicit_clean = df_json_clean_read.select(
    from_json(col("value"), user_schema).alias("parsed")
).select("parsed.*")

print("\nWith clean data and explicit schema:")
df_explicit_clean.printSchema()
df_explicit_clean.show()

print("\n✅ Benefits of explicit schema:")
print("  - Correct data types enforced")
print("  - Fast read (no inference)")
print("  - Fails fast on bad data")
print("  - Serves as documentation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Error Handling Patterns
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC Production ingestion must handle errors gracefully:
# MAGIC
# MAGIC **Error Types:**
# MAGIC 1. **Schema Errors**: Data doesn't match expected structure
# MAGIC 2. **Data Quality Errors**: Values violate business rules
# MAGIC 3. **System Errors**: Network issues, permission problems
# MAGIC
# MAGIC **Handling Strategies:**
# MAGIC - **Fail Fast**: Stop on first error (good for critical data)
# MAGIC - **Quarantine**: Move bad records to error table
# MAGIC - **Permissive**: Accept partial data, null out bad fields
# MAGIC - **Rescue**: Capture malformed records in special column

# COMMAND ----------

print("=== Error Handling Pattern 1: PERMISSIVE Mode ===\n")

# Create data with errors
mixed_quality_data = [
    {"order_id": 1, "amount": 100.50, "customer_id": 1001},
    {"order_id": 2, "amount": "invalid", "customer_id": 1002},  # Bad amount
    {"order_id": 3, "amount": 250.75, "customer_id": 1003},
    {"order_id": 4, "customer_id": 1004}  # Missing amount
]

# Write mixed quality JSON data to Delta table
json_strings_mixed = [json.dumps(record) for record in mixed_quality_data]
df_json_mixed = spark.createDataFrame([Row(value=js) for js in json_strings_mixed])

temp_mixed_table = get_table_path("bronze", "temp_error_handling_data")
df_json_mixed.write.format("delta").mode("overwrite").saveAsTable(temp_mixed_table)

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("amount", DoubleType(), True),
    StructField("customer_id", IntegerType(), False)
])

# PERMISSIVE mode (default): null out bad values
# Read Delta table and parse JSON using from_json
df_json = spark.table(temp_mixed_table)
df_permissive = df_json.withColumn(
    "parsed",
    from_json(col("value"), order_schema)
).select("parsed.*")

print("PERMISSIVE mode result:")
df_permissive.show()

print("⚠️ PERMISSIVE mode:")
print("  - Bad 'amount' values become null")
print("  - Processing continues")
print("  - Silent data quality issues possible")

# COMMAND ----------

print("=== Error Handling Pattern 2: DROPMALFORMED Mode ===\n")

""" 
 DROPMALFORMED: Skip bad records entirely
 To use DROPMALFORMED mode with the DataFrame API, we can write the parsed JSON strings to a file
 and then read it back. This is a workaround for environments where RDDs are restricted.
 DROPMALFORMED: Skip bad records entirely using from_json 
"""

temp_json_path = f"{VOLUME_PATH}temp_json_for_dropmalformed"
spark.table(temp_mixed_table).select("value").write.format("text").mode("overwrite").save(temp_json_path)

df_dropmalformed = spark.table(temp_mixed_table) \
    .select(from_json(col("value"), order_schema).alias("parsed_json")) \
    .select("parsed_json.*") \
    .na.drop()

print("DROPMALFORMED mode result:")
df_dropmalformed.show()

print("✅ DROPMALFORMED mode:")
print("  - Completely removes bad records")
print("  - Only clean data processed")
print("  - Data loss possible (track separately)")

# COMMAND ----------

print("=== Error Handling Pattern 3: Rescue Column (Recommended) ===\n")

# Parse JSON and capture malformed records in a rescue column
df = spark.table(temp_mixed_table)
df_rescue = df.withColumn(
    "parsed",
    from_json(col("value"), order_schema)
).withColumn(
    "_rescued_data",
    col("value")
).where(col("parsed").isNotNull() | col("parsed").isNull())

print("Rescue column result:")
df_rescue.show(truncate=False)

# Separate good and bad records
df_good = df_rescue.filter(col("_rescued_data").isNull()).drop("_rescued_data")
df_bad = df_rescue.filter(col("_rescued_data").isNotNull())

print(f"\nGood records: {df_good.count()}")
df_good.show()

print(f"\nBad records (quarantined): {df_bad.count()}")
df_bad.show(truncate=False)

print("\n✅ Rescue column pattern:")
print("  - Captures all malformed records")
print("  - Allows separate handling of errors")
print("  - No data loss")
print("  - Enables error analysis and alerting")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Idempotent Ingestion
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Idempotent Operation**: Running the same operation multiple times produces the same result
# MAGIC
# MAGIC **Why It Matters:**
# MAGIC - Jobs may fail and retry
# MAGIC - Network issues can cause duplicate messages
# MAGIC - Reprocessing historical data should be safe
# MAGIC
# MAGIC **Key Pattern**: Use MERGE (upsert) instead of INSERT
# MAGIC
# MAGIC **Without Idempotency:**
# MAGIC ```
# MAGIC Run 1: Insert 100 records → Total: 100
# MAGIC Run 2: Insert 100 records → Total: 200 (duplicates!)
# MAGIC ```
# MAGIC
# MAGIC **With Idempotency:**
# MAGIC ```
# MAGIC Run 1: Merge 100 records → Total: 100
# MAGIC Run 2: Merge 100 records → Total: 100 (no duplicates)
# MAGIC ```

# COMMAND ----------

from delta.tables import DeltaTable

print("=== Non-Idempotent Ingestion (PROBLEM) ===\n")

# Initial customer data
initial_customers = [
    (1, "Alice", "alice@example.com", 100.0),
    (2, "Bob", "bob@example.com", 150.0),
    (3, "Carol", "carol@example.com", 200.0)
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("total_spent", DoubleType(), False)
])

df_initial = spark.createDataFrame(initial_customers, customer_schema)
non_idempotent_table = get_table_path("bronze", "non_idempotent_customers")

# First load
df_initial.write.format("delta").mode("overwrite").saveAsTable(non_idempotent_table)
print("First load:")
spark.table(non_idempotent_table).show()

# Simulated "retry" with same data (using append - BAD!)
print("\nSimulated retry (using append - non-idempotent):")
df_initial.write.format("delta").mode("append").saveAsTable(non_idempotent_table)

print("Result after retry:")
result = spark.table(non_idempotent_table)
result.show()
print(f"❌ Total records: {result.count()} (should be 3, got duplicates!)")

# COMMAND ----------

print("=== Idempotent Ingestion with MERGE (SOLUTION) ===\n")

# Fresh start - Using Unity Catalog
idempotent_table = get_table_path("bronze", "idempotent_customers")
df_initial.write.format("delta").mode("overwrite").saveAsTable(idempotent_table)

print("Initial data:")
spark.table(idempotent_table).show()

# Incoming data (same records, simulating retry)
incoming_customers = [
    (1, "Alice", "alice@example.com", 100.0),  # Duplicate
    (2, "Bob", "bob@example.com", 150.0),      # Duplicate
    (3, "Carol", "carol@example.com", 200.0),  # Duplicate
    (4, "David", "david@example.com", 175.0)   # New record
]

df_incoming = spark.createDataFrame(incoming_customers, customer_schema)

# MERGE operation (idempotent)
delta_table = DeltaTable.forName(spark, idempotent_table)

delta_table.alias("target").merge(
    df_incoming.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("\nAfter MERGE (idempotent operation):")
result_idempotent = spark.table(idempotent_table)
result_idempotent.orderBy("customer_id").show()

print(f"✅ Total records: {result_idempotent.count()} (correct - no duplicates!)")

# Run merge again to prove idempotency
print("\nRunning MERGE again with same data:")
delta_table.alias("target").merge(
    df_incoming.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

result_verify = spark.table(idempotent_table)
print(f"Total records: {result_verify.count()} (still 4 - idempotent!)")

print("\n✅ Idempotent ingestion achieved:")
print("  - Same operation, same result every time")
print("  - Safe to retry on failure")
print("  - No duplicate records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Incremental Loading Strategies
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Full Load**: Load entire dataset every time
# MAGIC - Simple but inefficient
# MAGIC - Works for small datasets
# MAGIC - Not scalable
# MAGIC
# MAGIC **Incremental Load**: Only load new/changed data
# MAGIC - Efficient for large datasets
# MAGIC - Requires tracking mechanism
# MAGIC - More complex but scalable
# MAGIC
# MAGIC **Tracking Mechanisms:**
# MAGIC 1. **Timestamp-based**: Track `last_modified` or `created_at`
# MAGIC 2. **Sequence-based**: Track `max_id` or sequence number
# MAGIC 3. **Change Data Capture (CDC)**: Database change streams
# MAGIC 4. **File-based**: Track processed file names

# COMMAND ----------

print("=== Incremental Load Pattern 1: Timestamp-Based ===\n")

# Source system with timestamped records
source_data_day1 = [
    (1, "Product A", 100.0, datetime(2024, 1, 15, 10, 0)),
    (2, "Product B", 150.0, datetime(2024, 1, 15, 11, 0)),
    (3, "Product C", 200.0, datetime(2024, 1, 15, 12, 0))
]

source_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("last_modified", TimestampType(), False)
])

df_day1 = spark.createDataFrame(source_data_day1, source_schema)
incremental_table = get_table_path("bronze", "incremental_products")

# Initial load - Using Unity Catalog
df_day1.write.format("delta").mode("overwrite").saveAsTable(incremental_table)
print("Day 1 - Initial load:")
spark.table(incremental_table).show()

# Track watermark (last successfully loaded timestamp)
watermark = df_day1.agg({"last_modified": "max"}).collect()[0][0]
print(f"Watermark after Day 1: {watermark}")

# COMMAND ----------

# Day 2: New and updated records
source_data_day2 = [
    (2, "Product B Updated", 160.0, datetime(2024, 1, 16, 9, 0)),   # Updated
    (3, "Product C", 200.0, datetime(2024, 1, 15, 12, 0)),          # No change
    (4, "Product D", 250.0, datetime(2024, 1, 16, 10, 0)),          # New
    (5, "Product E", 300.0, datetime(2024, 1, 16, 11, 0))           # New
]

df_day2 = spark.createDataFrame(source_data_day2, source_schema)

# Incremental load: Only process records after watermark
df_incremental = df_day2.filter(col("last_modified") > watermark)

print(f"\nDay 2 - Incremental load (only records after {watermark}):")
print(f"Records to process: {df_incremental.count()}")
df_incremental.show()

# Merge incremental data
delta_table = DeltaTable.forName(spark, incremental_table)
delta_table.alias("target").merge(
    df_incremental.alias("source"),
    "target.product_id = source.product_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("\nResult after incremental load:")
spark.table(incremental_table).orderBy("product_id").show()

# Update watermark
new_watermark = df_day2.agg({"last_modified": "max"}).collect()[0][0]
print(f"✅ New watermark: {new_watermark}")

print("\n✅ Timestamp-based incremental loading:")
print("  - Only processes changed data")
print("  - Efficient for large datasets")
print("  - Requires reliable timestamp column")

# COMMAND ----------

print("=== Incremental Load Pattern 2: Sequence-Based ===\n")

# Source with auto-incrementing ID
sequence_data_batch1 = [
    (1, "Order A", 100.0),
    (2, "Order B", 150.0),
    (3, "Order C", 200.0)
]

sequence_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_name", StringType(), False),
    StructField("amount", DoubleType(), False)
])

df_batch1 = spark.createDataFrame(sequence_data_batch1, sequence_schema)
sequence_table = get_table_path("bronze", "sequence_orders")

# Initial load - Using Unity Catalog
df_batch1.write.format("delta").mode("overwrite").saveAsTable(sequence_table)
print("Batch 1 - Initial load:")
spark.table(sequence_table).show()

# Track max ID
max_id = df_batch1.agg({"order_id": "max"}).collect()[0][0]
print(f"Max order_id after Batch 1: {max_id}")

# Batch 2: New orders
sequence_data_batch2 = [
    (3, "Order C", 200.0),    # Duplicate (ID <= max_id, will be filtered)
    (4, "Order D", 250.0),    # New
    (5, "Order E", 300.0),    # New
    (6, "Order F", 175.0)     # New
]

df_batch2 = spark.createDataFrame(sequence_data_batch2, sequence_schema)

# Incremental: Only IDs greater than max_id
df_new_orders = df_batch2.filter(col("order_id") > max_id)

print(f"\nBatch 2 - Incremental load (order_id > {max_id}):")
print(f"New records: {df_new_orders.count()}")
df_new_orders.show()

# Append new records
df_new_orders.write.format("delta").mode("append").saveAsTable(sequence_table)

print("\nResult after incremental load:")
spark.table(sequence_table).orderBy("order_id").show()

# Update max_id
new_max_id = df_batch2.agg({"order_id": "max"}).collect()[0][0]
print(f"✅ New max order_id: {new_max_id}")

print("\n✅ Sequence-based incremental loading:")
print("  - Simple to implement")
print("  - Works with auto-increment IDs")
print("  - Doesn't capture updates (only inserts)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### 1. Batch vs Streaming
# MAGIC
# MAGIC - **Batch**: Scheduled, high throughput, complete datasets
# MAGIC - **Streaming**: Continuous, low latency, incremental processing
# MAGIC - Choose based on latency requirements and data characteristics
# MAGIC
# MAGIC ### 2. Schema Strategies
# MAGIC
# MAGIC - **Always use explicit schemas in production**
# MAGIC - Schema inference is for exploration only
# MAGIC - Explicit schemas = performance + reliability + documentation
# MAGIC
# MAGIC ### 3. Error Handling
# MAGIC
# MAGIC - **PERMISSIVE**: Null out bad values (risky)
# MAGIC - **DROPMALFORMED**: Skip bad records (data loss)
# MAGIC - **Rescue column**: Best practice - capture and quarantine errors
# MAGIC
# MAGIC ### 4. Idempotent Ingestion
# MAGIC
# MAGIC - **Use MERGE instead of INSERT/APPEND**
# MAGIC - Safe to retry on failure
# MAGIC - No duplicate records
# MAGIC - Essential for production reliability
# MAGIC
# MAGIC ### 5. Incremental Loading
# MAGIC
# MAGIC - **Timestamp-based**: Track `last_modified` (captures updates)
# MAGIC - **Sequence-based**: Track `max_id` (inserts only)
# MAGIC - **CDC**: Database change streams (most complete)
# MAGIC - Choose based on source system capabilities
# MAGIC
# MAGIC ### Production Checklist
# MAGIC
# MAGIC - ✅ Define explicit schemas
# MAGIC - ✅ Implement error handling with rescue columns
# MAGIC - ✅ Use MERGE for idempotent operations
# MAGIC - ✅ Implement incremental loading
# MAGIC - ✅ Track watermarks/checkpoints
# MAGIC - ✅ Add data quality validations
# MAGIC - ✅ Monitor and alert on errors
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Practice with real data sources (S3, databases, APIs)
# MAGIC - Implement data quality checks
# MAGIC - Build end-to-end ingestion pipelines
# MAGIC - Learn about Change Data Feed (CDF) for incremental consumption

# COMMAND ----------

# Final
print("=== Tables Created ===")
print(f"\nExample tables created in your Unity Catalog schema: {USER_SCHEMA}")
print("  - bronze_batch_sales")
print("  - bronze_non_idempotent_customers")
print("  - bronze_idempotent_customers")
print("  - bronze_incremental_products")
print("  - bronze_sequence_orders")
print("\nThese tables persist in Unity Catalog and can be accessed across sessions")