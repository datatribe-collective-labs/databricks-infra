# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Design Patterns Explained - Week 4
# MAGIC
# MAGIC This notebook provides a practical exploration of data pipeline design patterns,
# MAGIC focusing on building robust, maintainable, and scalable data pipelines.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand the Medallion Architecture (Bronze-Silver-Gold)
# MAGIC - Learn idempotency and exactly-once processing
# MAGIC - Master data quality and validation patterns
# MAGIC - Explore monitoring and observability strategies
# MAGIC - Understand checkpointing and recovery patterns
# MAGIC
# MAGIC ## Topics Covered
# MAGIC
# MAGIC 1. Medallion Architecture Deep Dive
# MAGIC 2. Idempotency: Writing Safe Pipelines
# MAGIC 3. Data Quality Validation Patterns
# MAGIC 4. Monitoring and Observability
# MAGIC 5. Checkpointing and Recovery

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Medallion Architecture: Bronze-Silver-Gold
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC The Medallion Architecture organizes data into three layers based on quality:
# MAGIC
# MAGIC **Bronze Layer (Raw)**:
# MAGIC - Raw data "as-is" from source
# MAGIC - No transformations, just ingestion
# MAGIC - Includes all fields, even unused ones
# MAGIC - Serves as immutable source of truth
# MAGIC
# MAGIC **Silver Layer (Cleaned)**:
# MAGIC - Validated and cleaned data
# MAGIC - Schema enforcement
# MAGIC - Business logic applied
# MAGIC - Deduplicated and enriched
# MAGIC
# MAGIC **Gold Layer (Business)**:
# MAGIC - Aggregated, feature-rich tables
# MAGIC - Optimized for consumption
# MAGIC - Business-level aggregations
# MAGIC - Powering dashboards and reports
# MAGIC
# MAGIC ### Why This Pattern?
# MAGIC
# MAGIC - **Separation of Concerns**: Each layer has clear responsibility
# MAGIC - **Data Quality**: Progressive refinement
# MAGIC - **Replayability**: Bronze is immutable, can rebuild Silver/Gold
# MAGIC - **Performance**: Optimize each layer independently

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup.py

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, regexp_replace, trim, upper, sum as _sum, avg, count, when, lit
from datetime import datetime

print("=== Medallion Architecture Implementation ===\n")

# Simulate raw data from source (messy, inconsistent)
raw_orders_data = [
    {"order_id": "1001", "customer_email": " alice@example.com ", "product": "Laptop", "amount": "1299.99", "order_date": "2024-01-15 10:30:00", "status": "completed"},
    {"order_id": "1002", "customer_email": "BOB@EXAMPLE.COM", "product": "Mouse", "amount": "29.99", "order_date": "2024-01-15 11:45:00", "status": "completed"},
    {"order_id": "1003", "customer_email": "carol@example.com", "product": "Keyboard", "amount": "invalid", "order_date": "2024-01-15 14:20:00", "status": "pending"},  # Bad amount
    {"order_id": "1002", "customer_email": "BOB@EXAMPLE.COM", "product": "Mouse", "amount": "29.99", "order_date": "2024-01-15 11:45:00", "status": "completed"},  # Duplicate
    {"order_id": "1004", "customer_email": "david@example.com", "product": "Monitor", "amount": "399.99", "order_date": "2024-01-15 16:10:00", "status": "cancelled"}
]

print("RAW DATA from source (Bronze input):")
df_raw = spark.createDataFrame(raw_orders_data)
df_raw.show(truncate=False)

# COMMAND ----------

print("=== BRONZE LAYER: Store Raw Data ===\n")

bronze_table = get_table_path("bronze", "medallion_orders")

# Add metadata columns for lineage
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("order_service"))

df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print("✅ Bronze layer characteristics:")
print("  - Stores data exactly as received")
print("  - Added metadata (ingestion_timestamp, source_system)")
print("  - No transformations or validations")
print("  - Immutable source of truth")

df_bronze.show(truncate=False)

# COMMAND ----------

print("=== SILVER LAYER: Clean and Validate ===\n")

# Define explicit schema for Silver layer
silver_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_email", StringType(), False),
    StructField("product", StringType(), False),
    StructField("amount", DoubleType(), True),  # Nullable to handle bad data
    StructField("order_date", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("is_valid", BooleanType(), False)
])

# Read from Bronze
df_from_bronze = spark.table(bronze_table)

# Apply cleaning transformations
df_silver = (
    df_from_bronze
    .withColumn("order_id", col("order_id").cast("int")) 
    .withColumn("customer_email", trim(upper(col("customer_email"))))  
    # Replace non-numeric characters before casting 
    .withColumn("amount", 
        when(col("amount").rlike("^[0-9]*\\.?[0-9]+$"), col("amount").cast("double"))
        .otherwise(None)
    ) \
    .withColumn("order_date", to_timestamp(col("order_date"))) 
    .withColumn("is_valid",
        when(col("amount").isNull() | (col("amount") <= 0), False)
        .otherwise(True)
    ) \
    .dropDuplicates(["order_id"]) 
    .select("order_id", "customer_email", "product", "amount", "order_date", "status", "ingestion_timestamp", "is_valid"))

silver_table = get_table_path("silver", "medallion_orders")
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print("✅ Silver layer transformations:")
print("  - Type casting (order_id → int, amount → double)")
print("  - Data cleaning (trim, uppercase email)")
print("  - Deduplication (removed duplicate order_id 1002)")
print("  - Validation flag (is_valid based on business rules)")

df_silver.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, count, sum as _sum, avg
)

print("=== GOLD LAYER: Business Aggregations ===\n")

# Read from Silver
df_from_silver = spark.table(silver_table)

# Create business-level aggregation
df_gold_daily_summary = df_from_silver \
    .filter(col("is_valid") == True) \
    .filter(col("status") == "completed") \
    .groupBy(to_date(col("order_date")).alias("order_date")) \
    .agg(
        count("order_id").alias("total_orders"),
        _sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    ) \
    .orderBy("order_date")

gold_table = get_table_path("gold", "medallion_daily_summary")
df_gold_daily_summary.write.format("delta").mode("overwrite").saveAsTable(gold_table)

print("✅ Gold layer characteristics:")
print("  - Aggregated for business use")
print("  - Only valid, completed orders")
print("  - Optimized for analytics/BI tools")

df_gold_daily_summary.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import lit

print("=== Medallion Flow Summary ===\n")

print("BRONZE → SILVER → GOLD pipeline:")
print()
print("┌─────────────────────────────────────────┐")
print("│ BRONZE (Raw)                            │")
print("│ • Store as-is from source               │")
print("│ • Add metadata (timestamp, source)      │")
print("│ • Immutable historical record           │")
print("└────────────────┬────────────────────────┘")
print("                 ▼")
print("┌─────────────────────────────────────────┐")
print("│ SILVER (Cleaned)                        │")
print("│ • Type casting & validation             │")
print("│ • Deduplication                         │")
print("│ • Data quality flags                    │")
print("└────────────────┬────────────────────────┘")
print("                 ▼")
print("┌─────────────────────────────────────────┐")
print("│ GOLD (Business)                         │")
print("│ • Aggregations & features               │")
print("│ • Optimized for consumption             │")
print("│ • Powers dashboards/reports             │")
print("└─────────────────────────────────────────┘")

print("\n✅ Key benefit: If Gold needs recalculation, replay from Bronze!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Idempotency: Writing Safe Pipelines
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Idempotent Pipeline**: Produces same result regardless of how many times it runs
# MAGIC
# MAGIC **Why Critical:**
# MAGIC - Jobs fail and need to retry
# MAGIC - Data may arrive late or out of order
# MAGIC - Reprocessing historical data must be safe
# MAGIC
# MAGIC **Patterns for Idempotency:**
# MAGIC 1. **MERGE instead of INSERT**: Upsert pattern
# MAGIC 2. **Deterministic transformations**: Same input → same output
# MAGIC 3. **Transaction boundaries**: All-or-nothing operations
# MAGIC 4. **Checkpointing**: Track processed data

# COMMAND ----------

from delta.tables import DeltaTable

print("=== Non-Idempotent Pipeline (PROBLEM) ===\n")

# Initial events
events_batch1 = [
    (1, "user_login", "alice@example.com", datetime(2024, 1, 15, 10, 0)),
    (2, "page_view", "bob@example.com", datetime(2024, 1, 15, 10, 5)),
    (3, "purchase", "carol@example.com", datetime(2024, 1, 15, 10, 10))
]

events_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_email", StringType(), False),
    StructField("event_timestamp", TimestampType(), False)
])

df_batch1 = spark.createDataFrame(events_batch1, events_schema)
non_idempotent_table = get_table_path("bronze", "non_idempotent_events")

# Run 1: Append events
df_batch1.write.format("delta").mode("overwrite").saveAsTable(non_idempotent_table)
print("Run 1: Initial load")
spark.table(non_idempotent_table).show()

# Run 2: Retry with same data (using append - NOT idempotent)
print("\nRun 2: Retry with same data (append mode)")
df_batch1.write.format("delta").mode("append").saveAsTable(non_idempotent_table)

result = spark.table(non_idempotent_table)
print(f"❌ Result: {result.count()} records (should be 3, got duplicates!)")
result.show()

# COMMAND ----------

print("=== Idempotent Pipeline with MERGE (SOLUTION) ===\n")

# Fresh start
idempotent_table = get_table_path("bronze", "idempotent_events")
df_batch1.write.format("delta").mode("overwrite").saveAsTable(idempotent_table)

print("Run 1: Initial load")
spark.table(idempotent_table).show()

# Incoming batch (includes duplicates + new data)
events_batch2 = [
    (2, "page_view", "bob@example.com", datetime(2024, 1, 15, 10, 5)),    # Duplicate
    (3, "purchase", "carol@example.com", datetime(2024, 1, 15, 10, 10)),  # Duplicate
    (4, "user_login", "david@example.com", datetime(2024, 1, 15, 10, 15)), # New
    (5, "page_view", "eve@example.com", datetime(2024, 1, 15, 10, 20))     # New
]

df_batch2 = spark.createDataFrame(events_batch2, events_schema)

# MERGE operation (idempotent)
print("\nRun 2: Process batch with MERGE (idempotent)")
delta_events = DeltaTable.forName(spark, idempotent_table)

delta_events.alias("target").merge(
    df_batch2.alias("source"),
    "target.event_id = source.event_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

result_idempotent = spark.table(idempotent_table)
print(f"✅ Result: {result_idempotent.count()} records (correct!)")
result_idempotent.orderBy("event_id").show()

# Run again to prove idempotency
print("\nRun 3: Reprocess same batch (proving idempotency)")
delta_events.alias("target").merge(
    df_batch2.alias("source"),
    "target.event_id = source.event_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

result_verify = spark.table(idempotent_table)
print(f"✅ Result still: {result_verify.count()} records (idempotent!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Validation Patterns
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Data Quality Checks**: Validations to ensure data meets business requirements
# MAGIC
# MAGIC **Quality Dimensions:**
# MAGIC 1. **Completeness**: All required fields present
# MAGIC 2. **Accuracy**: Values within expected ranges
# MAGIC 3. **Consistency**: Cross-field validations
# MAGIC 4. **Timeliness**: Data freshness checks
# MAGIC 5. **Uniqueness**: No unwanted duplicates
# MAGIC
# MAGIC **Validation Strategies:**
# MAGIC - **Fail Fast**: Stop pipeline on critical errors
# MAGIC - **Quarantine**: Move bad records to error table
# MAGIC - **Repair**: Auto-fix known issues
# MAGIC - **Alert**: Continue but notify on warnings

# COMMAND ----------

print("=== Data Quality Validation Framework ===\n")

# Sample customer data with quality issues
customers_with_issues = [
    (1, "Alice Johnson", "alice@example.com", 25, 1500.0, datetime(2024, 1, 15)),
    (2, "Bob Smith", "invalid-email", 30, 2000.0, datetime(2024, 1, 16)),          # Invalid email
    (3, "Carol White", "carol@example.com", -5, 1800.0, datetime(2024, 1, 17)),    # Invalid age
    (4, None, "david@example.com", 28, 2200.0, datetime(2024, 1, 18)),             # Missing name
    (5, "Eve Davis", "eve@example.com", 35, 0.0, datetime(2024, 1, 19)),           # Suspicious amount
    (6, "Frank Miller", "frank@example.com", 150, 1900.0, datetime(2024, 1, 20)),  # Invalid age
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("lifetime_value", DoubleType(), True),
    StructField("signup_date", TimestampType(), False)
])

df_customers = spark.createDataFrame(customers_with_issues, customer_schema)

print("Input data with quality issues:")
df_customers.show(truncate=False)

# COMMAND ----------

print("=== Quality Check 1: Completeness ===\n")

# Check for null required fields
df_with_completeness = df_customers \
    .withColumn("completeness_check",
        when(col("customer_id").isNull(), "FAIL: customer_id is null")
        .when(col("name").isNull(), "FAIL: name is null")
        .when(col("email").isNull(), "FAIL: email is null")
        .otherwise("PASS")
    )

print("Completeness validation:")
df_with_completeness.select("customer_id", "name", "email", "completeness_check").show(truncate=False)

failed_completeness = df_with_completeness.filter(col("completeness_check").startswith("FAIL")).count()
print(f"Failed completeness: {failed_completeness} records")

# COMMAND ----------

print("=== Quality Check 2: Accuracy (Range Validation) ===\n")

# Validate age and lifetime_value ranges
df_with_accuracy = df_with_completeness \
    .withColumn("accuracy_check",
        when((col("age") < 0) | (col("age") > 120), "FAIL: age out of valid range (0-120)")
        .when(col("lifetime_value") < 0, "FAIL: negative lifetime value")
        .when(col("lifetime_value") == 0, "WARN: zero lifetime value")
        .otherwise("PASS")
    )

print("Accuracy validation:")
df_with_accuracy.select("customer_id", "age", "lifetime_value", "accuracy_check").show(truncate=False)

failed_accuracy = df_with_accuracy.filter(col("accuracy_check").startswith("FAIL")).count()
warnings_accuracy = df_with_accuracy.filter(col("accuracy_check").startswith("WARN")).count()
print(f"Failed accuracy: {failed_accuracy} records")
print(f"Warnings: {warnings_accuracy} records")

# COMMAND ----------

print("=== Quality Check 3: Consistency (Pattern Validation) ===\n")

# Email format validation
df_with_consistency = df_with_accuracy \
    .withColumn("consistency_check",
        when(~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"),
             "FAIL: invalid email format")
        .otherwise("PASS")
    )

print("Consistency validation:")
df_with_consistency.select("customer_id", "email", "consistency_check").show(truncate=False)

failed_consistency = df_with_consistency.filter(col("consistency_check").startswith("FAIL")).count()
print(f"Failed consistency: {failed_consistency} records")

# COMMAND ----------

from pyspark.sql.functions import concat_ws

print("=== Combined Quality Score ===\n")

# Combine all validations
df_quality_scored = df_with_consistency \
    .withColumn("quality_issues",
        concat_ws("; ",
            when(col("completeness_check") != "PASS", col("completeness_check")),
            when(col("accuracy_check") != "PASS", col("accuracy_check")),
            when(col("consistency_check") != "PASS", col("consistency_check"))
        )
    ) \
    .withColumn("quality_status",
        when(col("quality_issues").contains("FAIL"), "FAILED")
        .when(col("quality_issues").contains("WARN"), "WARNING")
        .otherwise("PASSED")
    )

print("Quality summary:")
df_quality_scored.select("customer_id", "quality_status", "quality_issues").show(truncate=False)

# Split into good and bad data
df_good = df_quality_scored.filter(col("quality_status") == "PASSED")
df_bad = df_quality_scored.filter(col("quality_status") == "FAILED")
df_warning = df_quality_scored.filter(col("quality_status") == "WARNING")

print(f"\n✅ Quality summary:")
print(f"  PASSED: {df_good.count()} records → Write to Silver")
print(f"  WARNING: {df_warning.count()} records → Write to Silver with flag")
print(f"  FAILED: {df_bad.count()} records → Quarantine to error table")

# Write to appropriate locations
good_table = get_table_path("silver", "quality_good_customers")
bad_table = get_table_path("bronze", "quality_quarantine")

df_good.write.format("delta").mode("overwrite").saveAsTable(good_table)
df_bad.write.format("delta").mode("overwrite").saveAsTable(bad_table)

print("\n✅ Data quality pattern applied:")
print("  - Good data → Silver layer")
print("  - Bad data → Quarantine for investigation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monitoring and Observability
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Observability**: Understanding what's happening inside your pipeline
# MAGIC
# MAGIC **Key Metrics to Track:**
# MAGIC 1. **Volume Metrics**: Records processed, bytes read/written
# MAGIC 2. **Quality Metrics**: Pass/fail rates, error counts
# MAGIC 3. **Performance Metrics**: Processing time, throughput
# MAGIC 4. **Business Metrics**: Revenue, customers, transactions
# MAGIC
# MAGIC **Monitoring Approaches:**
# MAGIC - **Metrics Table**: Store run statistics
# MAGIC - **Event Log**: Track all operations
# MAGIC - **Alerting**: Notify on anomalies

# COMMAND ----------

print("=== Pipeline Monitoring Pattern ===\n")

# Define metrics schema
metrics_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),
    StructField("records_input", IntegerType(), True),
    StructField("records_output", IntegerType(), True),
    StructField("records_failed", IntegerType(), True),
    StructField("error_message", StringType(), True)
])

import uuid
from datetime import datetime

# Simulate pipeline run
run_id = str(uuid.uuid4())
pipeline_name = "customer_silver_pipeline"
start_time = datetime.now()

print(f"Pipeline run started: {run_id}")

# Simulate processing
input_count = df_customers.count()
good_count = df_good.count()
failed_count = df_bad.count()

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

# Record metrics
metrics_data = [(
    run_id,
    pipeline_name,
    start_time,
    end_time,
    "SUCCESS",
    input_count,
    good_count,
    failed_count,
    None
)]

df_metrics = spark.createDataFrame(metrics_data, metrics_schema)

print("\nPipeline metrics:")
df_metrics.show(truncate=False)

# Write to metrics table
metrics_table = get_table_path("bronze", "pipeline_metrics")
df_metrics.write.format("delta").mode("append").saveAsTable(metrics_table)

print(f"✅ Metrics logged:")
print(f"  Duration: {duration:.2f} seconds")
print(f"  Input: {input_count} records")
print(f"  Success: {good_count} records")
print(f"  Failed: {failed_count} records")
print(f"  Success Rate: {(good_count/input_count*100):.1f}%")

# COMMAND ----------

print("=== Data Quality Metrics Over Time ===\n")

# Simulate multiple runs with different quality scores
historical_runs = [
    (str(uuid.uuid4()), "customer_silver_pipeline", datetime(2024, 1, 15, 10, 0), datetime(2024, 1, 15, 10, 5), "SUCCESS", 1000, 950, 50, None),
    (str(uuid.uuid4()), "customer_silver_pipeline", datetime(2024, 1, 16, 10, 0), datetime(2024, 1, 16, 10, 4), "SUCCESS", 1000, 920, 80, None),
    (str(uuid.uuid4()), "customer_silver_pipeline", datetime(2024, 1, 17, 10, 0), datetime(2024, 1, 17, 10, 6), "SUCCESS", 1000, 880, 120, None),
]

df_historical = spark.createDataFrame(historical_runs, metrics_schema)
df_historical.write.format("delta").mode("append").saveAsTable(metrics_table)

# Analyze quality trends
df_all_metrics = spark.table(metrics_table)

df_quality_trend = df_all_metrics \
    .withColumn("success_rate", (col("records_output") / col("records_input") * 100).cast("decimal(5,2)")) \
    .withColumn("failure_rate", (col("records_failed") / col("records_input") * 100).cast("decimal(5,2)")) \
    .select("start_time", "records_input", "records_output", "records_failed", "success_rate", "failure_rate") \
    .orderBy("start_time")

print("Quality trend analysis:")
df_quality_trend.show(truncate=False)

# Alert on quality degradation
latest_failure_rate = df_quality_trend.orderBy(col("start_time").desc()).select("failure_rate").first()[0]
print(f"\n⚠️ Alert check:")
print(f"  Latest failure rate: {latest_failure_rate}%")
if latest_failure_rate > 10:
    print("  🚨 ALERT: Failure rate exceeded 10% threshold!")
else:
    print("  ✅ Quality within acceptable range")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checkpointing and Recovery
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Checkpointing**: Tracking what's been processed to enable recovery
# MAGIC
# MAGIC **Why Needed:**
# MAGIC - Jobs fail mid-processing
# MAGIC - Need to resume from last successful point
# MAGIC - Avoid reprocessing entire dataset
# MAGIC
# MAGIC **Checkpoint Strategies:**
# MAGIC 1. **Watermark-based**: Track max timestamp processed
# MAGIC 2. **Offset-based**: Track position in stream/queue
# MAGIC 3. **File-based**: Track processed file names
# MAGIC 4. **Structured Streaming**: Built-in checkpoint location

# COMMAND ----------

print("=== Checkpoint Pattern: Watermark-Based ===\n")

# Simulate incremental data processing with checkpoints
checkpoint_schema = StructType([
    StructField("pipeline_name", StringType(), False),
    StructField("checkpoint_type", StringType(), False),
    StructField("checkpoint_value", StringType(), False),
    StructField("updated_at", TimestampType(), False)
])

# Initial data load
initial_transactions = [
    (1, "txn_001", datetime(2024, 1, 15, 10, 0), 100.0),
    (2, "txn_002", datetime(2024, 1, 15, 11, 0), 150.0),
    (3, "txn_003", datetime(2024, 1, 15, 12, 0), 200.0),
]

txn_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("txn_code", StringType(), False),
    StructField("txn_timestamp", TimestampType(), False),
    StructField("amount", DoubleType(), False)
])

df_txn_batch1 = spark.createDataFrame(initial_transactions, txn_schema)
txn_table = get_table_path("bronze", "transactions")

# Process batch 1
df_txn_batch1.write.format("delta").mode("overwrite").saveAsTable(txn_table)

# Get watermark (max timestamp processed)
watermark = df_txn_batch1.agg({"txn_timestamp": "max"}).collect()[0][0]
print(f"Batch 1 processed. Watermark: {watermark}")

# Save checkpoint
checkpoint_data = [("transaction_pipeline", "watermark", str(watermark), datetime.now())]
df_checkpoint = spark.createDataFrame(checkpoint_data, checkpoint_schema)

checkpoint_table = get_table_path("bronze", "checkpoints")
df_checkpoint.write.format("delta").mode("overwrite").saveAsTable(checkpoint_table)

print("✅ Checkpoint saved")

# COMMAND ----------

# New batch arrives
print("\n=== Processing New Batch with Checkpoint ===\n")

new_transactions = [
    (3, "txn_003", datetime(2024, 1, 15, 12, 0), 200.0),  # Duplicate (before watermark)
    (4, "txn_004", datetime(2024, 1, 15, 13, 0), 250.0),  # New
    (5, "txn_005", datetime(2024, 1, 15, 14, 0), 300.0),  # New
    (6, "txn_006", datetime(2024, 1, 15, 15, 0), 175.0),  # New
]

df_txn_batch2 = spark.createDataFrame(new_transactions, txn_schema)

# Read checkpoint
df_checkpoint_read = spark.table(checkpoint_table)
last_watermark = df_checkpoint_read \
    .filter(col("pipeline_name") == "transaction_pipeline") \
    .filter(col("checkpoint_type") == "watermark") \
    .select("checkpoint_value") \
    .first()[0]

print(f"Last processed watermark: {last_watermark}")

# Only process records after watermark (incremental)
df_new_txn = df_txn_batch2.filter(col("txn_timestamp") > last_watermark)

print(f"\nNew transactions to process: {df_new_txn.count()}")
df_new_txn.show()

# Process new data
df_new_txn.write.format("delta").mode("append").saveAsTable(txn_table)

# Update checkpoint
new_watermark = df_txn_batch2.agg({"txn_timestamp": "max"}).collect()[0][0]
checkpoint_update = [("transaction_pipeline", "watermark", str(new_watermark), datetime.now())]
df_checkpoint_new = spark.createDataFrame(checkpoint_update, checkpoint_schema)
df_checkpoint_new.write.format("delta").mode("overwrite").saveAsTable(checkpoint_table)

print(f"\n✅ New watermark saved: {new_watermark}")

# COMMAND ----------

print("=== Recovery Scenario: Job Failure ===\n")

# Simulate job failure mid-processing
print("Scenario: Job crashes after processing batch 2")
print("  1. Job restarts")
print("  2. Reads checkpoint")
print("  3. Determines what to reprocess")

# Read checkpoint on restart
df_checkpoint_recovery = spark.table(checkpoint_table)
recovery_watermark = df_checkpoint_recovery \
    .filter(col("pipeline_name") == "transaction_pipeline") \
    .select("checkpoint_value") \
    .first()[0]

print(f"\n✅ Recovery watermark: {recovery_watermark}")

# Reprocess from checkpoint
# (In real scenario, would query source system for data after watermark)
print("  - Only fetch data with txn_timestamp > recovery_watermark")
print("  - No duplicate processing")
print("  - Efficient recovery")

# Verify final state
df_final = spark.table(txn_table)
print(f"\nFinal transaction count: {df_final.count()}")
df_final.orderBy("txn_timestamp").show()

print("\n✅ Checkpoint pattern benefits:")
print("  - Resume from last successful point")
print("  - No duplicate processing")
print("  - Automatic recovery on job restart")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### 1. Medallion Architecture
# MAGIC
# MAGIC - **Bronze**: Raw, immutable source of truth
# MAGIC - **Silver**: Cleaned, validated, deduplicated
# MAGIC - **Gold**: Aggregated, business-ready
# MAGIC - **Benefit**: Can rebuild downstream from upstream layers
# MAGIC
# MAGIC ### 2. Idempotency
# MAGIC
# MAGIC - **Use MERGE, not INSERT/APPEND**
# MAGIC - Safe to retry on failure
# MAGIC - Same input → same output (deterministic)
# MAGIC - Critical for production reliability
# MAGIC
# MAGIC ### 3. Data Quality Validation
# MAGIC
# MAGIC - **Check completeness**: Required fields present
# MAGIC - **Check accuracy**: Values in valid ranges
# MAGIC - **Check consistency**: Pattern/format validation
# MAGIC - **Quarantine bad data**: Don't pollute Silver layer
# MAGIC - **Track quality metrics**: Monitor trends over time
# MAGIC
# MAGIC ### 4. Monitoring and Observability
# MAGIC
# MAGIC - **Track metrics**: Volume, quality, performance
# MAGIC - **Log all runs**: Pipeline execution history
# MAGIC - **Alert on anomalies**: Quality degradation, failures
# MAGIC - **Business metrics**: Revenue, customers, etc.
# MAGIC
# MAGIC ### 5. Checkpointing and Recovery
# MAGIC
# MAGIC - **Track progress**: Watermarks, offsets, file names
# MAGIC - **Enable recovery**: Resume from checkpoint
# MAGIC - **Avoid reprocessing**: Incremental processing only
# MAGIC - **Structured Streaming**: Built-in checkpoint support
# MAGIC
# MAGIC ### Production Pipeline Checklist
# MAGIC
# MAGIC - ✅ Implement Medallion architecture (Bronze-Silver-Gold)
# MAGIC - ✅ Ensure idempotency (use MERGE)
# MAGIC - ✅ Add data quality validations
# MAGIC - ✅ Quarantine bad data (error handling)
# MAGIC - ✅ Implement monitoring (metrics table)
# MAGIC - ✅ Add checkpointing (recovery mechanism)
# MAGIC - ✅ Set up alerting (quality/failure thresholds)
# MAGIC - ✅ Document schema and business logic
# MAGIC - ✅ Test failure scenarios
# MAGIC - ✅ Monitor in production (Spark UI, metrics)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Build end-to-end Medallion pipeline
# MAGIC - Implement comprehensive data quality framework
# MAGIC - Set up monitoring dashboards
# MAGIC - Practice failure recovery scenarios
# MAGIC - Learn Change Data Feed (CDF) for incremental processing

# COMMAND ----------

# Final
print("=== Tables Created ===")
print(f"\nExample tables created in {CATALOG}.{USER_SCHEMA}:")
print("  - bronze_medallion_orders (Medallion Bronze layer)")
print("  - silver_medallion_orders (Medallion Silver layer)")
print("  - gold_medallion_daily_summary (Medallion Gold layer)")
print("  - bronze_non_idempotent_events (Idempotency problem demo)")
print("  - bronze_idempotent_events (Idempotency solution with MERGE)")
print("  - silver_quality_good_customers (Quality validation passed)")
print("  - bronze_quality_quarantine (Quality validation failed)")
print("  - bronze_pipeline_metrics (Monitoring and metrics)")
print("  - bronze_transactions (Checkpoint watermark demo)")
print("  - bronze_checkpoints (Checkpoint tracking)")