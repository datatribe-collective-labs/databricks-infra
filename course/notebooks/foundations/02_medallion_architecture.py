# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture
# MAGIC 
# MAGIC ## What is Medallion Architecture?
# MAGIC 
# MAGIC Medallion Architecture is a data design pattern used to logically organize data in a lakehouse. It consists of three layers:
# MAGIC 
# MAGIC - **🥉 Bronze (Raw)**: Data as it arrives, minimal processing
# MAGIC - **🥈 Silver (Refined)**: Cleaned, validated, and enriched data
# MAGIC - **🥇 Gold (Curated)**: Business-level aggregates and features
# MAGIC 
# MAGIC ```
# MAGIC Source Systems → Bronze → Silver → Gold → BI/Analytics
# MAGIC    (IoT, APIs)    (Raw)   (Clean)  (Aggregated)
# MAGIC ```
# MAGIC 
# MAGIC ## Why Use Medallion Architecture?
# MAGIC 
# MAGIC 1. **Separation of Concerns**: Each layer has a clear purpose
# MAGIC 2. **Data Quality Gates**: Validate and clean progressively
# MAGIC 3. **Performance**: Optimize each layer for its use case
# MAGIC 4. **Reusability**: Silver tables serve multiple Gold tables
# MAGIC 5. **Debugging**: Easy to trace issues back through layers
# MAGIC 6. **Auditability**: Raw data always preserved in Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥉 Bronze Layer: Raw Data Landing Zone
# MAGIC 
# MAGIC ### Purpose
# MAGIC - Store data exactly as it arrives from source systems
# MAGIC - Minimal or no transformation
# MAGIC - Immutable and append-only
# MAGIC - Acts as a historical archive
# MAGIC 
# MAGIC ### Characteristics
# MAGIC - Often schema-on-read (JSON, CSV, Parquet)
# MAGIC - Includes metadata: ingestion timestamp, source file name
# MAGIC - May contain duplicates or bad records
# MAGIC - Partitioned by ingestion date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# Sample Bronze data - Raw meter readings from IoT devices
bronze_data = [
    ('{"device_id": "MTR001", "timestamp": 1699920000, "reading": 125.5, "quality": "good"}', "iot-stream-1", "2023-11-14 08:00:00"),
    ('{"device_id": "MTR002", "timestamp": 1699920000, "reading": 89.3, "quality": "good"}', "iot-stream-1", "2023-11-14 08:00:00"),
    ('{"device_id": "MTR001", "timestamp": 1699923600, "reading": 126.8, "quality": "good"}', "iot-stream-2", "2023-11-14 09:00:00"),
    ('{"device_id": "MTR002", "timestamp": 1699923600, "reading": null, "quality": "error"}', "iot-stream-2", "2023-11-14 09:00:00"),  # Bad record
    ('{"device_id": "MTR003", "timestamp": 1699920000, "reading": 210.4, "quality": "good"}', "iot-stream-1", "2023-11-14 08:00:00"),
    ('{"device_id": "MTR001", "timestamp": 1699923600, "reading": 126.8, "quality": "good"}', "iot-stream-2", "2023-11-14 09:00:00"),  # Duplicate
]

bronze_schema = StructType([
    StructField("raw_payload", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True)
])

bronze_df = spark.createDataFrame(bronze_data, bronze_schema)

print("🥉 BRONZE LAYER - Raw IoT Data")
display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Observations
# MAGIC 
# MAGIC ✅ **Good practices we see:**
# MAGIC - Raw JSON payload preserved exactly as received
# MAGIC - Source tracking (`source_file`) for lineage
# MAGIC - Ingestion timestamp for debugging
# MAGIC 
# MAGIC ⚠️ **Issues present (expected at this layer):**
# MAGIC - Duplicate records (MTR001 at 1699923600 appears twice)
# MAGIC - Null reading for MTR002
# MAGIC - Data quality issues not yet addressed
# MAGIC - JSON strings, not parsed yet

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥈 Silver Layer: Cleaned and Validated Data
# MAGIC 
# MAGIC ### Purpose
# MAGIC - Parse and validate Bronze data
# MAGIC - Remove duplicates
# MAGIC - Handle data quality issues
# MAGIC - Apply business rules and standardization
# MAGIC - Create conformed dimensions
# MAGIC 
# MAGIC ### Characteristics
# MAGIC - Strongly typed schemas (no JSON strings)
# MAGIC - Deduplication logic applied
# MAGIC - Data quality checks enforced
# MAGIC - Slowly changing dimensions (SCDs) maintained
# MAGIC - Ready for analytics use cases

# COMMAND ----------

from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, row_number
from pyspark.sql.window import Window

# Define schema for parsing JSON
json_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("reading", DoubleType(), True),
    StructField("quality", StringType(), True)
])

# Parse JSON and transform to Silver
silver_df = bronze_df \
    .withColumn("parsed", from_json(col("raw_payload"), json_schema)) \
    .select(
        col("parsed.device_id").alias("meter_id"),
        to_timestamp(from_unixtime(col("parsed.timestamp"))).alias("reading_timestamp"),
        col("parsed.reading").alias("energy_consumption_kwh"),
        col("parsed.quality").alias("data_quality_flag"),
        col("ingestion_timestamp").alias("processed_at")
    ) \
    .filter(col("data_quality_flag") == "good") \
    .filter(col("energy_consumption_kwh").isNotNull())

# Deduplicate based on meter_id and reading_timestamp
window_spec = Window.partitionBy("meter_id", "reading_timestamp").orderBy(col("processed_at").desc())
silver_df = silver_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

print("🥈 SILVER LAYER - Cleaned Meter Readings")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Improvements
# MAGIC 
# MAGIC ✅ **Transformations applied:**
# MAGIC - JSON parsed into strongly-typed columns
# MAGIC - Duplicates removed (MTR001 duplicate gone)
# MAGIC - Bad quality records filtered out (null reading removed)
# MAGIC - Unix timestamp converted to proper TIMESTAMP type
# MAGIC - Business-friendly column names
# MAGIC 
# MAGIC **This data is now ready for analytics!**
# MAGIC 
# MAGIC Silver tables typically include:
# MAGIC - `fact_*` tables (events, transactions)
# MAGIC - `dim_*` tables (customers, products, locations)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create some dimension tables for Silver layer:

# COMMAND ----------

# Sample Customer Dimension (Silver)
customer_data = [
    ("CUST_A", "ABC Manufacturing", "Industrial", "Helsinki", "Finland", "2020-01-15"),
    ("CUST_B", "Tech Startup Oy", "Commercial", "Espoo", "Finland", "2021-06-20"),
    ("CUST_C", "Residential User", "Residential", "Vantaa", "Finland", "2022-03-10"),
]

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("customer_type", StringType(), False),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_since", StringType(), True)
])

dim_customer = spark.createDataFrame(customer_data, customer_schema)

print("🥈 SILVER LAYER - Customer Dimension")
display(dim_customer)

# COMMAND ----------

# Sample Meter Dimension (Silver)
meter_data = [
    ("MTR001", "CUST_A", "Smart Meter v2", "2020-02-01", True),
    ("MTR002", "CUST_B", "Smart Meter v2", "2021-07-01", True),
    ("MTR003", "CUST_C", "Smart Meter v3", "2022-04-01", True),
]

meter_schema = StructType([
    StructField("meter_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("meter_type", StringType(), True),
    StructField("installation_date", StringType(), True),
    StructField("is_active", StringType(), True)
])

dim_meter = spark.createDataFrame(meter_data, meter_schema)

print("🥈 SILVER LAYER - Meter Dimension")
display(dim_meter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Business-Level Aggregates
# MAGIC 
# MAGIC ### Purpose
# MAGIC - Aggregated, business-level data
# MAGIC - Optimized for specific use cases
# MAGIC - Pre-joined tables for common queries
# MAGIC - Calculated metrics and KPIs
# MAGIC - Powers dashboards and reports
# MAGIC 
# MAGIC ### Characteristics
# MAGIC - Highly denormalized (fewer joins needed)
# MAGIC - Project or department-specific
# MAGIC - Contains derived metrics
# MAGIC - Optimized for BI tool consumption
# MAGIC - May have multiple Gold tables from same Silver sources

# COMMAND ----------

# Gold Layer Example 1: Daily Consumption by Customer
gold_daily_consumption = silver_df \
    .join(dim_meter, "meter_id") \
    .join(dim_customer, "customer_id") \
    .withColumn("reading_date", to_timestamp(col("reading_timestamp")).cast("date")) \
    .groupBy("reading_date", "customer_id", "customer_name", "customer_type") \
    .agg(
        {"energy_consumption_kwh": "sum"}
    ) \
    .withColumnRenamed("sum(energy_consumption_kwh)", "total_daily_consumption_kwh") \
    .orderBy("reading_date", "customer_id")

print("🥇 GOLD LAYER - Daily Consumption by Customer")
display(gold_daily_consumption)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Example 2: Customer Summary Statistics

# COMMAND ----------

from pyspark.sql.functions import count, avg, sum as _sum, min as _min, max as _max

gold_customer_summary = silver_df \
    .join(dim_meter, "meter_id") \
    .join(dim_customer, "customer_id") \
    .groupBy("customer_id", "customer_name", "customer_type", "city") \
    .agg(
        count("*").alias("total_readings"),
        _sum("energy_consumption_kwh").alias("total_consumption_kwh"),
        avg("energy_consumption_kwh").alias("avg_consumption_kwh"),
        _min("reading_timestamp").alias("first_reading"),
        _max("reading_timestamp").alias("last_reading")
    ) \
    .orderBy("total_consumption_kwh", ascending=False)

print("🥇 GOLD LAYER - Customer Summary")
display(gold_customer_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Benefits
# MAGIC 
# MAGIC ✅ **Optimized for consumption:**
# MAGIC - Pre-aggregated metrics (no runtime calculation)
# MAGIC - Pre-joined tables (no complex joins in BI tool)
# MAGIC - Business-friendly column names
# MAGIC - Specific to use case (daily dashboard, executive summary)
# MAGIC 
# MAGIC **Performance impact:**
# MAGIC - BI queries run in milliseconds, not seconds
# MAGIC - No need for data analysts to understand complex joins
# MAGIC - Consistent calculations across all reports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer Comparison Summary
# MAGIC 
# MAGIC | Aspect | Bronze 🥉 | Silver 🥈 | Gold 🥇 |
# MAGIC |--------|-----------|-----------|---------|
# MAGIC | **Purpose** | Archive raw data | Clean & validate | Business aggregates |
# MAGIC | **Schema** | Loose/JSON | Strongly typed | Denormalized |
# MAGIC | **Quality** | May have issues | Quality assured | Production-ready |
# MAGIC | **Audience** | Data engineers | Data engineers/analysts | Business users |
# MAGIC | **Optimization** | Ingestion speed | Query flexibility | Specific use cases |
# MAGIC | **Grain** | As received | Atomic | Aggregated |
# MAGIC | **Updates** | Append-only | Upserts allowed | Recalculated |
# MAGIC | **Lineage** | Source system | Bronze layer | Silver layer |

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Add More Layers?
# MAGIC 
# MAGIC ### Additional Layers You Might See:
# MAGIC 
# MAGIC **Landing/Raw Zone (Before Bronze)**
# MAGIC - For very large files that need pre-processing
# MAGIC - Temporary staging before Bronze
# MAGIC 
# MAGIC **Platinum Layer (After Gold)**
# MAGIC - ML feature stores
# MAGIC - Real-time serving layers
# MAGIC - Extremely optimized for specific models
# MAGIC 
# MAGIC ### Keep It Simple!
# MAGIC 
# MAGIC ⚠️ **Don't over-engineer:**
# MAGIC - Start with Bronze → Silver → Gold
# MAGIC - Add layers only when there's clear need
# MAGIC - Each layer adds complexity and latency
# MAGIC - More layers ≠ better architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Medallion Architecture
# MAGIC 
# MAGIC ### 1. **Bronze Layer**
# MAGIC ```python
# MAGIC # Always include metadata
# MAGIC .withColumn("_ingestion_time", current_timestamp())
# MAGIC .withColumn("_source_file", input_file_name())
# MAGIC 
# MAGIC # Partition by ingestion date
# MAGIC .write.partitionBy("_ingestion_date")
# MAGIC ```
# MAGIC 
# MAGIC ### 2. **Silver Layer**
# MAGIC ```python
# MAGIC # Apply data quality checks
# MAGIC .filter(col("amount") > 0)
# MAGIC .filter(col("date").isNotNull())
# MAGIC 
# MAGIC # Deduplicate
# MAGIC .dropDuplicates(["id", "timestamp"])
# MAGIC 
# MAGIC # Use Delta Lake for ACID guarantees
# MAGIC .write.format("delta").mode("append")
# MAGIC ```
# MAGIC 
# MAGIC ### 3. **Gold Layer**
# MAGIC ```python
# MAGIC # Pre-join related tables
# MAGIC fact.join(dim_customer).join(dim_product)
# MAGIC 
# MAGIC # Pre-calculate metrics
# MAGIC .withColumn("revenue", col("quantity") * col("price"))
# MAGIC 
# MAGIC # Optimize for query patterns
# MAGIC .write.format("delta")
# MAGIC .option("optimizeWrite", "true")
# MAGIC .partitionBy("year", "month")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-World Data Flow Example
# MAGIC 
# MAGIC ```
# MAGIC IoT Meter → Kafka → Bronze (raw JSON)
# MAGIC                         ↓
# MAGIC                    Parse & Clean
# MAGIC                         ↓
# MAGIC                    Silver (facts & dims)
# MAGIC                    ├─ fact_meter_readings
# MAGIC                    ├─ dim_customer
# MAGIC                    └─ dim_meter
# MAGIC                         ↓
# MAGIC                    Aggregate & Join
# MAGIC                         ↓
# MAGIC                    Gold (use case specific)
# MAGIC                    ├─ daily_consumption_by_customer
# MAGIC                    ├─ monthly_billing_summary
# MAGIC                    └─ realtime_usage_dashboard
# MAGIC                         ↓
# MAGIC                    Power BI / Tableau
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Anti-Patterns to Avoid
# MAGIC 
# MAGIC ### ❌ Skipping Bronze Layer
# MAGIC - "We'll just clean data on ingestion"
# MAGIC - Problem: Can't debug or replay if cleaning logic changes
# MAGIC 
# MAGIC ### ❌ Putting Aggregates in Silver
# MAGIC - Silver should be atomic/granular data
# MAGIC - Save aggregations for Gold
# MAGIC 
# MAGIC ### ❌ Too Many Layers
# MAGIC - Bronze → Silver-1 → Silver-2 → Silver-3 → Gold-1 → Gold-2
# MAGIC - Creates complexity without value
# MAGIC 
# MAGIC ### ❌ No Clear Layer Boundaries
# MAGIC - Mixing raw and cleaned data
# MAGIC - Confusion about which table to use
# MAGIC 
# MAGIC ### ❌ Not Using Delta Lake Features
# MAGIC - Time travel, ACID transactions, schema evolution
# MAGIC - These features make medallion architecture powerful!

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC 
# MAGIC Now that you understand the layer structure, we'll dive into:
# MAGIC 
# MAGIC 1. **Dimensional Modeling** - How to design fact and dimension tables in Silver
# MAGIC 2. **SCDs and Delta Patterns** - Handling changes over time with Delta Lake
# MAGIC 
# MAGIC These concepts will help you build robust Silver and Gold layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Key Takeaways
# MAGIC 
# MAGIC - Medallion architecture provides clear separation of concerns
# MAGIC - **Bronze**: Raw, immutable, archived
# MAGIC - **Silver**: Cleaned, validated, analytical-ready
# MAGIC - **Gold**: Aggregated, optimized for specific use cases
# MAGIC - Each layer has different optimization goals
# MAGIC - Start simple (3 layers) and add only when needed
# MAGIC - Use Delta Lake features for ACID guarantees
# MAGIC - Clear naming conventions: `bronze.*`, `silver.*`, `gold.*`
