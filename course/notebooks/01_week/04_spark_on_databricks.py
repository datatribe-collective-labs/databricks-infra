# Databricks notebook source
# MAGIC %md
# MAGIC # Spark on Databricks Deep Dive - Week 01
# MAGIC 
# MAGIC This notebook provides comprehensive coverage of Apache Spark fundamentals within the Databricks environment, including distributed computing concepts, Spark APIs, optimization techniques, and best practices.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC - Understand Spark architecture and distributed computing concepts
# MAGIC - Master DataFrame API and Spark SQL
# MAGIC - Learn optimization techniques and performance tuning
# MAGIC - Explore streaming and advanced Spark features
# MAGIC - Implement best practices for production workloads

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apache Spark Architecture
# MAGIC 
# MAGIC ### Spark Cluster Architecture
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                        Driver Program                       │
# MAGIC │  ┌─────────────────────────────────────────────────────────┐ │
# MAGIC │  │                  SparkContext                          │ │
# MAGIC │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │ │
# MAGIC │  │  │    DAG      │  │   Task      │  │    Spark SQL    │ │ │
# MAGIC │  │  │  Scheduler  │  │  Scheduler  │  │     Engine      │ │ │
# MAGIC │  │  └─────────────┘  └─────────────┘  └─────────────────┘ │ │
# MAGIC │  └─────────────────────────────────────────────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Cluster Manager                          │
# MAGIC │            (Databricks Managed Clusters)                   │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                      Worker Nodes                           │
# MAGIC │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
# MAGIC │  │   Worker 1  │    │   Worker 2  │    │    Worker N     │ │
# MAGIC │  │ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────────┐ │ │
# MAGIC │  │ │Executor │ │    │ │Executor │ │    │ │  Executor   │ │ │
# MAGIC │  │ │ ┌─────┐ │ │    │ │ ┌─────┐ │ │    │ │ ┌─────────┐ │ │ │
# MAGIC │  │ │ │Task │ │ │    │ │ │Task │ │ │    │ │ │  Task   │ │ │ │
# MAGIC │  │ │ └─────┘ │ │    │ │ └─────┘ │ │    │ │ └─────────┘ │ │ │
# MAGIC │  │ └─────────┘ │    │ └─────────┘ │    │ └─────────────┘ │ │
# MAGIC │  └─────────────┘    └─────────────┘    └─────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

print("=== Spark Environment Analysis ===")

# Get SparkSession information
print(f"Spark Version: {spark.version}")
print(f"SparkSession: {spark}")


# Accessing Spark JVM driver with SparkContext is not possible with serverless compute, 
# but only with Databricks Runtime on a cluster.

# print(f"SparkContext App Name: {spark.sparkContext.appName}")
# print(f"SparkContext Master: {spark.sparkContext.master}")
# print(f"SparkContext: {spark.sparkContext}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark DataFrames and Datasets
# MAGIC 
# MAGIC ### DataFrame Benefits in Databricks
# MAGIC 
# MAGIC 1. **Catalyst Optimizer**: Query optimization and code generation
# MAGIC 2. **Tungsten Engine**: Memory management and code generation
# MAGIC 3. **Columnar Storage**: Efficient data processing with Parquet/Delta
# MAGIC 4. **Schema Enforcement**: Type safety and data validation
# MAGIC 5. **Language Integration**: Seamless Python, Scala, SQL, R support

# COMMAND ----------

# Create comprehensive sample dataset for DataFrame operations
print("=== DataFrame Operations Deep Dive ===")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, date
import decimal
import random

# Create a realistic e-commerce dataset
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_category", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("discount_amount", DecimalType(10, 2), True),
    StructField("transaction_date", DateType(), False),
    StructField("customer_segment", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("shipping_cost", DecimalType(8, 2), True)
])

# Generate sample data
sample_data = []
categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty']
segments = ['Premium', 'Standard', 'Budget']
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']

for i in range(1000):
    category = random.choice(categories)
    unit_price = decimal.Decimal(str(round(random.uniform(10.0, 500.0), 2)))
    discount_amount = (
        decimal.Decimal(str(round(random.uniform(0.0, 50.0), 2)))
        if random.random() > 0.7 else None
    )
    shipping_cost = (
        decimal.Decimal(str(round(random.uniform(5.0, 25.0), 2)))
        if random.random() > 0.8 else None
    )
    sample_data.append((
        f"TXN{i+1:06d}",
        random.randint(1001, 9999),
        f"PROD{random.randint(1, 500):03d}",
        category,
        f"{category} Product {random.randint(1, 100)}",
        random.randint(1, 5),
        unit_price,
        discount_amount,
        date(2024, random.randint(1, 12), random.randint(1, 28)),
        random.choice(segments),
        random.choice(payment_methods),
        shipping_cost
    ))

# Create DataFrame
df = spark.createDataFrame(sample_data, schema)
print(f"Created DataFrame with {df.count()} transactions")
df.printSchema()

# COMMAND ----------

# DataFrame transformations and actions
print("=== DataFrame Transformations ===")

# 1. Basic transformations
print("1. Basic Transformations:")

# Select and rename columns
selected_df = df.select(
    F.col("transaction_id").alias("txn_id"),
    "customer_id",
    "product_category",
    "quantity",
    "unit_price"
)
print(f"   Selected columns: {selected_df.columns}")

# Filter operations
high_value_df = df.filter(F.col("unit_price") > 100)
print(f"   High-value transactions: {high_value_df.count()}")

# Add computed columns
enhanced_df = df.withColumn(
    "total_amount", 
    F.col("quantity") * F.col("unit_price")
).withColumn(
    "discounted_amount",
    F.when(F.col("discount_amount").isNotNull(), 
           (F.col("quantity") * F.col("unit_price")) - F.col("discount_amount"))
    .otherwise(F.col("quantity") * F.col("unit_price"))
)

print("   Added computed columns: total_amount, discounted_amount")

# COMMAND ----------

# Advanced DataFrame operations
print("=== Advanced DataFrame Operations ===")

# 2. Aggregations
print("2. Aggregations:")

# Group by operations
category_summary = enhanced_df.groupBy("product_category") \
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_transaction"),
        F.count("transaction_id").alias("transaction_count"),
        F.max("total_amount").alias("max_transaction")
    ) \
    .orderBy(F.desc("total_revenue"))

print("   Category Performance Summary:")
category_summary.show(truncate=False)

# 3. Window functions
print("\n3. Window Functions:")

from pyspark.sql.window import Window

# Customer ranking by total spending
customer_window = Window.partitionBy("customer_segment").orderBy(F.desc("total_amount"))

customer_analysis = enhanced_df \
    .withColumn("customer_rank", F.row_number().over(customer_window)) \
    .withColumn("running_total", F.sum("total_amount").over(
        customer_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ))

print("   Top customers by segment:")
customer_analysis.filter(F.col("customer_rank") <= 3) \
    .select("customer_id", "customer_segment", "total_amount", "customer_rank") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL Integration
# MAGIC 
# MAGIC Databricks provides seamless integration between DataFrame API and SQL:

# COMMAND ----------

# SQL Integration demonstration
print("=== Spark SQL Integration ===")

# Register DataFrame as temporary view
enhanced_df.createOrReplaceTempView("transactions")

# SQL queries
print("1. SQL Query Examples:")

# Complex analytical query
sql_result = spark.sql("""
    SELECT 
        product_category,
        customer_segment,
        COUNT(*) as transaction_count,
        ROUND(AVG(total_amount), 2) as avg_amount,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) as revenue_percentage
    FROM transactions
    GROUP BY product_category, customer_segment
    ORDER BY total_revenue DESC
""")

print("   Revenue Analysis by Category and Segment:")
sql_result.show(10, truncate=False)

# Time-based analysis
monthly_trends = spark.sql("""
    SELECT 
        YEAR(transaction_date) as year,
        MONTH(transaction_date) as month,
        COUNT(*) as transactions,
        SUM(total_amount) as monthly_revenue
    FROM transactions
    GROUP BY YEAR(transaction_date), MONTH(transaction_date)
    ORDER BY year, month
""")

print("\n   Monthly Revenue Trends:")
monthly_trends.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Techniques
# MAGIC 
# MAGIC ### Catalyst Optimizer Features
# MAGIC 
# MAGIC 1. **Predicate Pushdown**: Filters applied at data source
# MAGIC 2. **Projection Pushdown**: Only required columns read
# MAGIC 3. **Constant Folding**: Compile-time expression evaluation
# MAGIC 4. **Boolean Expression Simplification**: Logical optimization
# MAGIC 5. **Join Reordering**: Optimal join execution order

# COMMAND ----------

# Performance optimization examples
print("=== Performance Optimization Techniques ===")

# 1. Caching strategies
print("1. Caching Strategies:")

# Cache frequently accessed data
frequently_used = enhanced_df.filter(F.col("customer_segment") == "Premium")
print(f"   Cached premium customer data: {frequently_used.count()} records")

# Storage levels
from pyspark import StorageLevel
memory_and_disk = enhanced_df.filter(F.col("total_amount") > 200)
print(f"   Persisted high-value transactions: {memory_and_disk.count()} records")

# 2. Partitioning optimization
print("\n2. Partitioning Optimization:")

print(f"   Original partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Repartition by key for better join performance
partitioned_df = enhanced_df.repartition(4, "product_category")
print(f"   After repartitioning by category: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Coalesce to reduce partitions for small datasets
coalesced_df = enhanced_df.coalesce(2)
print(f"   After coalescing: {spark.conf.get('spark.sql.shuffle.partitions')}")

# 3. Broadcast joins
print("\n3. Broadcast Join Optimization:")

# Create a small lookup table
category_info = spark.createDataFrame([
    ("Electronics", "Tech", "High"),
    ("Clothing", "Fashion", "Medium"), 
    ("Books", "Education", "Low"),
    ("Home & Garden", "Lifestyle", "Medium"),
    ("Sports", "Fitness", "Medium"),
    ("Beauty", "Personal Care", "High")
], ["category", "department", "margin"])

# Broadcast the small table for efficient joins
from pyspark.sql.functions import broadcast

enriched_data = enhanced_df.join(
    broadcast(category_info),
    enhanced_df.product_category == category_info.category,
    "left"
).drop("category")

print(f"   Enriched data with broadcast join: {enriched_data.count()} records")
print("   Added department and margin information")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adaptive Query Execution (AQE)
# MAGIC 
# MAGIC Databricks Runtime includes Adaptive Query Execution for dynamic optimization:
# MAGIC 
# MAGIC ### AQE Features
# MAGIC 
# MAGIC 1. **Dynamic Coalescing**: Reduces number of partitions post-shuffle
# MAGIC 2. **Dynamic Join Strategy**: Switches join algorithms based on runtime statistics
# MAGIC 3. **Dynamic Skew Handling**: Splits skewed partitions automatically
# MAGIC 4. **Dynamic Pruning**: Prunes partitions during runtime

# COMMAND ----------

# Demonstrate AQE features
print("=== Adaptive Query Execution (AQE) ===")

# Demonstrate query optimization with explain
print("\n1. Query Execution Plan Analysis:")

# Create a query that benefits from AQE
complex_query = enriched_data.filter(F.col("total_amount") > 50) \
    .groupBy("department", "customer_segment") \
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("transaction_count")
    ) \
    .orderBy(F.desc("total_revenue"))

# Show logical and physical plans
print("   Logical Plan:")
complex_query.explain(extended=False)

print("\n2. Performance Monitoring:")
import time

start_time = time.time()
result = complex_query.collect()
execution_time = time.time() - start_time

print(f"   Query execution time: {execution_time:.3f} seconds")
print(f"   Result rows: {len(result)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Spark Features
# MAGIC 
# MAGIC ### User Defined Functions (UDFs)
# MAGIC 
# MAGIC While built-in functions are preferred for performance, UDFs provide flexibility for custom logic:

# COMMAND ----------

# UDF examples
print("=== User Defined Functions (UDFs) ===")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

# 1. Python UDF
def categorize_amount(amount):
    """Categorize transaction amounts"""
    if amount < 50:
        return "Low"
    elif amount < 200:
        return "Medium"
    else:
        return "High"

categorize_udf = udf(categorize_amount, StringType())

# 2. Pandas UDF (vectorized - better performance)

# Key features:
#  - Pandas UDF is vectorized: Instead of processing one row at a time, it processes
#    a batch of rows as a pandas.Series (or pandas.DataFrame for multiple cols).
#  - Use of Apache Arrow: Arrow is the high-speed, in-memory columnar data 
#    format used to move that pandas.Series between Spark (JVM) and this 
#    Python function without expensive serialization/deserialization.

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(returnType=FloatType())
def calculate_loyalty_score(amounts: pd.Series) -> pd.Series:
    """Calculate customer loyalty score based on transaction amounts"""
    return amounts.astype(float).apply(lambda x: min(100.0, x / 10.0))

# Apply UDFs
df_with_udfs = enhanced_df.select(
    "*",
    categorize_udf("total_amount").alias("amount_category"),
    calculate_loyalty_score("total_amount").alias("loyalty_score")
)

print("Applied UDFs:")
df_with_udfs.select("total_amount", "amount_category", "loyalty_score").show(10)

# Performance tip: Built-in functions are usually faster
df_builtin = enhanced_df.withColumn(
    "amount_category_builtin",
    F.when(F.col("total_amount") < 50, "Low")
     .when(F.col("total_amount") < 200, "Medium")
     .otherwise("High")
)

print("\nBuilt-in function equivalent (preferred for performance):")
df_builtin.select("total_amount", "amount_category_builtin").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured Streaming Concepts
# MAGIC 
# MAGIC While this is a batch processing example, Databricks supports Structured Streaming:
# MAGIC 
# MAGIC ### Streaming Features
# MAGIC 
# MAGIC 1. **Exactly-Once Processing**: Guarantees for fault tolerance
# MAGIC 2. **Late Data Handling**: Watermarking for event-time processing
# MAGIC 3. **Output Modes**: Append, Update, Complete
# MAGIC 4. **Checkpointing**: State management for recovery

# COMMAND ----------

# Streaming simulation (conceptual)
print("=== Structured Streaming Concepts ===")

# Simulate streaming data processing patterns
print("1. Streaming Processing Patterns:")

# Windowed aggregations (batch simulation)
from pyspark.sql.functions import window

# Add timestamp for windowing example
df_with_timestamp = enhanced_df.withColumn(
    "event_time", 
    F.to_timestamp(F.col("transaction_date"))
)

# Simulate windowed aggregation
windowed_sales = df_with_timestamp \
    .groupBy(
        window(F.col("event_time"), "7 days"),
        "product_category"
    ) \
    .agg(
        F.sum("total_amount").alias("weekly_revenue"),
        F.count("*").alias("weekly_transactions")
    ) \
    .orderBy("window")

print("   Weekly Revenue by Category (windowed aggregation):")
windowed_sales.show(10, truncate=False)

# 2. State management concepts
print("\n2. State Management Patterns:")

# Simulate stateful operations
customer_state = enhanced_df.groupBy("customer_id") \
    .agg(
        F.sum("total_amount").alias("lifetime_value"),
        F.count("*").alias("total_transactions"),
        F.max("transaction_date").alias("last_transaction"),
        F.avg("total_amount").alias("avg_transaction_amount")
    )

print("   Customer State Summary:")
customer_state.orderBy(F.desc("lifetime_value")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Production
# MAGIC 
# MAGIC ### Code Organization
# MAGIC 
# MAGIC ```python
# MAGIC # 1. Modular functions
# MAGIC def process_transactions(df):
# MAGIC     return df.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
# MAGIC 
# MAGIC # 2. Configuration management
# MAGIC config = {
# MAGIC     "input_path": "/mnt/data/transactions",
# MAGIC     "output_path": "/mnt/data/processed",
# MAGIC     "partition_columns": ["year", "month"],
# MAGIC     "checkpoint_location": "/mnt/checkpoints"
# MAGIC }
# MAGIC 
# MAGIC # 3. Error handling
# MAGIC try:
# MAGIC     result = spark.read.parquet(config["input_path"])
# MAGIC except Exception as e:
# MAGIC     logger.error(f"Failed to read data: {e}")
# MAGIC     raise
# MAGIC ```

# COMMAND ----------

# Production best practices demonstration
print("=== Production Best Practices ===")

# 1. Data quality checks
print("1. Data Quality Validation:")

def validate_data_quality(df):
    """Perform basic data quality checks"""
    total_rows = df.count()
    
    # Null checks
    null_checks = {}
    for column in df.columns:
        null_count = df.filter(F.col(column).isNull()).count()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        null_checks[column] = {
            "null_count": null_count,
            "null_percentage": round(null_percentage, 2)
        }
    
    return null_checks

# Run quality checks
quality_report = validate_data_quality(enhanced_df)
print("   Data Quality Report:")
for column, stats in quality_report.items():
    if stats["null_percentage"] > 0:
        print(f"     {column}: {stats['null_count']} nulls ({stats['null_percentage']}%)")

# 2. Performance monitoring
print("\n2. Performance Monitoring:")

def monitor_query_performance(df, operation_name):
    """Monitor query execution performance"""
    start_time = time.time()
    
    # Execute query
    result_count = df.count()
    
    execution_time = time.time() - start_time
    
    # Log performance metrics
    metrics = {
        "operation": operation_name,
        "execution_time": round(execution_time, 3),
        "rows_processed": result_count,
        "rows_per_second": round(result_count / execution_time, 2) if execution_time > 0 else 0
    }
    
    return metrics

# Monitor performance
perf_metrics = monitor_query_performance(enhanced_df, "data_processing")
print(f"   Operation: {perf_metrics['operation']}")
print(f"   Execution Time: {perf_metrics['execution_time']} seconds")
print(f"   Rows Processed: {perf_metrics['rows_processed']}")
print(f"   Throughput: {perf_metrics['rows_per_second']} rows/second")

# 3. Resource cleanup
print("\n3. Resource Management:")

# Clear temporary views
spark.sql("DROP VIEW IF EXISTS transactions")
print("   Dropped temporary views")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debugging and Troubleshooting
# MAGIC 
# MAGIC ### Common Issues and Solutions
# MAGIC 
# MAGIC | Issue | Symptoms | Solutions |
# MAGIC |-------|----------|-----------|
# MAGIC | **OutOfMemory** | Executor failures, GC pressure | Increase memory, reduce partition size |
# MAGIC | **Data Skew** | Few tasks taking much longer | Salting, custom partitioning |
# MAGIC | **Shuffle Spill** | Slow performance, disk I/O | Increase shuffle partitions |
# MAGIC | **Small Files** | Many small partitions | Coalesce, optimize file sizes |

# COMMAND ----------

# Debugging tools demonstration
print("=== Debugging and Troubleshooting Tools ===")

# 1. Partition analysis
def analyze_partitions(df, sample_size=100):
    """Analyze partition distribution"""
    
    # Get partition count
    partition_count = spark.conf.get('spark.sql.shuffle.partitions')
    
    # Sample partition sizes
    from pyspark.sql.functions import spark_partition_id, count

    partition_sizes = (df
        .groupBy(spark_partition_id().alias("partition_id"))
        .agg(count("*").alias("row_count"))
        .orderBy("partition_id")
        .collect()
    )

    partition_sizes = [(row.partition_id, row.row_count) for row in partition_sizes]
    
    analysis = {
        "partition_count": partition_count,
        "total_rows": sum([partition_size[1] for partition_size in partition_sizes]),
        "avg_partition_size": sum([partition_size[1] for partition_size in partition_sizes]) / len(partition_sizes) if partition_sizes else 0,
        "min_partition_size": min([partition_size[1] for partition_size in partition_sizes]) if partition_sizes else 0,
        "max_partition_size": max([partition_size[1] for partition_size in partition_sizes]) if partition_sizes else 0,
        "size_distribution": partition_sizes
    }
    
    return analysis

# Analyze current DataFrame
partition_analysis = analyze_partitions(enhanced_df)
print("1. Partition Analysis:")
print(f"   Total Partitions: {partition_analysis['partition_count']}")
print(f"   Average Partition Size: {partition_analysis['avg_partition_size']:.0f} rows")
print(f"   Min/Max Sizes: {partition_analysis['min_partition_size']}/{partition_analysis['max_partition_size']} rows")

# Calculate skew
if partition_analysis['avg_partition_size'] > 0:
    skew_ratio = partition_analysis['max_partition_size'] / partition_analysis['avg_partition_size']
    print(f"   Skew Ratio: {skew_ratio:.2f}")
    
    if skew_ratio > 2.0:
        print("   ⚠️  High partition skew detected!")

# 2. Query plan analysis
print("\n2. Query Execution Plan:")

# Create a complex query for plan analysis
complex_aggregation = enhanced_df.join(category_info, 
                                     enhanced_df.product_category == category_info.category) \
    .groupBy("department") \
    .agg(F.sum("total_amount").alias("dept_revenue")) \
    .orderBy(F.desc("dept_revenue"))

# Show execution plan
print("   Execution plan for complex aggregation:")
complex_aggregation.explain(mode="simple")

# COMMAND ----------

print("=== Spark on Databricks Deep Dive Complete ===")
print("\nKey Takeaways:")
print("1. Spark provides distributed processing with automatic optimization")
print("2. DataFrame API offers both performance and ease of use")
print("3. Catalyst optimizer and AQE provide automatic query optimization")
print("4. Proper partitioning and caching strategies improve performance")
print("5. Built-in functions are preferred over UDFs for performance")
print("6. Monitor and debug using partition analysis and execution plans")
print("\nNext: Continue to 05_delta_lake_concepts_explained for schema management and CRUD operations with Delta Lake.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC 
# MAGIC ### Spark Documentation
# MAGIC - [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# MAGIC - [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
# MAGIC - [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC 
# MAGIC ### Databricks Resources
# MAGIC - [Databricks Runtime Release Notes](https://docs.databricks.com/release-notes/runtime/index.html)
# MAGIC - [Adaptive Query Execution](https://docs.databricks.com/optimizations/aqe.html)
# MAGIC - [Delta Lake Performance Tuning](https://docs.databricks.com/optimizations/index.html)