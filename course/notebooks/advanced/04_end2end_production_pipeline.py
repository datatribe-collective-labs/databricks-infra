# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Production Pipeline: E-Commerce Analytics
# MAGIC
# MAGIC ## Real-World Scenario
# MAGIC
# MAGIC You're a data engineer at an e-commerce company. Your task is to build a production pipeline that:
# MAGIC - Ingests sales transactions from multiple sources (files, APIs, databases)
# MAGIC - Cleans and validates the data
# MAGIC - Creates aggregated analytics tables for the BI team
# MAGIC - Ensures data quality and monitors pipeline health
# MAGIC
# MAGIC This notebook demonstrates a **complete production pipeline** using the `end2end` framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC DATA SOURCES
# MAGIC ├── CSV Files (transaction history)
# MAGIC ├── REST API (real-time orders)
# MAGIC └── Database (customer data)
# MAGIC         ↓
# MAGIC    BRONZE LAYER (Raw data)
# MAGIC         ↓
# MAGIC    SILVER LAYER (Cleaned, validated)
# MAGIC         ↓
# MAGIC     GOLD LAYER (Business metrics)
# MAGIC ```
# MAGIC
# MAGIC ### Pipeline Tasks
# MAGIC 1. **Ingest Sales** - Load historical sales from CSV
# MAGIC 2. **Ingest API Orders** - Fetch recent orders from API
# MAGIC 3. **Clean Sales** - Transform Bronze → Silver with validation
# MAGIC 4. **Aggregate Daily** - Create daily sales summaries (Silver → Gold)
# MAGIC 5. **Aggregate Product** - Create product performance metrics (Silver → Gold)
# MAGIC 6. **Quality Check** - Validate data quality across layers
# MAGIC 7. **Optimize Tables** - Optimize Delta Lake tables

# COMMAND ----------

# Setup: Add framework to path (for development)
import sys
import os

course_root = os.path.abspath("../../..")
src_path = os.path.join(course_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

# Import framework
from end2end import (
    PipelineConfig,
    FileIngestion,
    BronzeToSilver,
    SilverToGold,
    DataQualityCheck,
    Pipeline,
    Task,
    get_logger,
    optimize_table,
)
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp
import json

logger = get_logger(__name__)
print("✓ Framework loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set up pipeline configuration for your user schema.

# COMMAND ----------

# Get current user
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_schema = user_email.split("@")[0].replace(".", "_").lower()

# Create configuration
config = PipelineConfig(
    catalog="databricks_course",
    source_schema=user_schema,
    target_schema=user_schema,
    environment="dev",
    metadata={
        "pipeline": "ecommerce_analytics",
        "version": "1.0.0",
        "owner": user_email,
    }
)

print(f"User: {user_email}")
print(f"Schema: {user_schema}")
print(f"Config: {config}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Generation
# MAGIC
# MAGIC Generate sample e-commerce data for demonstration.

# COMMAND ----------

# DBTITLE 1,Generate Sample Sales Data
from pyspark.sql.functions import rand, expr, date_sub
from datetime import datetime, timedelta

# Generate sample sales transactions
def generate_sample_sales(num_rows=1000):
    """Generate sample sales data."""
    logger.info(f"Generating {num_rows} sample sales transactions")

    df = spark.range(num_rows) \
        .withColumn("transaction_id", expr("concat('TXN-', id)")) \
        .withColumn("customer_id", expr("concat('CUST-', cast(floor(rand() * 100) as string))")) \
        .withColumn("product_id", expr("concat('PROD-', cast(floor(rand() * 50) as string))")) \
        .withColumn("amount", expr("round(rand() * 500 + 10, 2)")) \
        .withColumn("quantity", expr("cast(floor(rand() * 5) + 1 as int)")) \
        .withColumn("transaction_date", expr("date_sub(current_date(), cast(floor(rand() * 30) as int))")) \
        .withColumn("status", expr("case when rand() > 0.9 then 'cancelled' else 'completed' end")) \
        .drop("id")

    return df

# Generate and preview data
sample_df = generate_sample_sales(1000)
display(sample_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Save Sample Data to Bronze (Simulating Ingestion)
# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{config.source_schema}")

# Save sample data to Bronze layer
bronze_table = config.get_table_path("bronze", "sales_transactions")
sample_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"✓ Sample data saved to: {bronze_table}")
print(f"Rows: {sample_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Task Definitions
# MAGIC
# MAGIC Define all tasks that will be orchestrated by the pipeline.

# COMMAND ----------

# DBTITLE 1,Task 1: Ingest Sales Transactions
def task_ingest_sales():
    """
    Ingest historical sales transactions from files.

    Returns dict with ingestion metrics.
    """
    logger.info("Starting sales ingestion...")

    # In production, this would read from actual file source
    # For demo, we're using the sample data already in Bronze
    bronze_table = config.get_table_path("bronze", "sales_transactions")
    df = spark.table(bronze_table)
    row_count = df.count()

    logger.info(f"✓ Ingested {row_count} sales transactions")

    return {
        "task": "ingest_sales",
        "rows_ingested": row_count,
        "target_table": bronze_table,
        "status": "success"
    }

# COMMAND ----------

# DBTITLE 1,Task 2: Clean and Validate (Bronze → Silver)
def task_clean_sales():
    """
    Clean and validate sales data (Bronze → Silver).

    Transformations:
    - Remove duplicates
    - Filter cancelled transactions
    - Validate required columns
    - Standardize IDs
    - Add quality metrics
    """
    logger.info("Starting Bronze → Silver transformation...")

    # Read from Bronze
    bronze_table = config.get_table_path("bronze", "sales_transactions")
    df = spark.table(bronze_table)

    original_count = df.count()

    # Apply transformations
    # 1. Remove cancelled transactions
    df = df.filter(col("status") == "completed")

    # 2. Remove duplicates
    df = df.dropDuplicates(["transaction_id"])

    # 3. Standardize IDs
    df = df.withColumn("customer_id", expr("upper(trim(customer_id))"))
    df = df.withColumn("product_id", expr("upper(trim(product_id))"))

    # 4. Add Silver metadata
    df = df.withColumn("silver_processed_at", current_timestamp())

    # 5. Validate
    if df.isEmpty():
        raise ValueError("No valid transactions after cleaning")

    # Write to Silver
    silver_table = config.get_table_path("silver", "sales_transactions")
    df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    cleaned_count = df.count()
    filtered_count = original_count - cleaned_count

    logger.info(f"✓ Cleaned {original_count} → {cleaned_count} rows ({filtered_count} filtered)")

    return {
        "task": "clean_sales",
        "rows_read": original_count,
        "rows_written": cleaned_count,
        "rows_filtered": filtered_count,
        "target_table": silver_table,
        "status": "success"
    }

# COMMAND ----------

# DBTITLE 1,Task 3: Aggregate Daily Sales (Silver → Gold)
def task_aggregate_daily_sales():
    """
    Aggregate daily sales metrics (Silver → Gold).

    Metrics:
    - Total revenue per day
    - Average order value
    - Transaction count
    - Total quantity sold
    """
    logger.info("Starting daily sales aggregation...")

    # Read from Silver
    silver_table = config.get_table_path("silver", "sales_transactions")
    df = spark.table(silver_table)

    # Aggregate by date
    daily_sales = df.groupBy("transaction_date") \
        .agg(
            expr("sum(amount) as total_revenue"),
            expr("avg(amount) as avg_order_value"),
            expr("count(*) as transaction_count"),
            expr("sum(quantity) as total_quantity")
        ) \
        .withColumn("gold_processed_at", current_timestamp())

    # Write to Gold
    gold_table = config.get_table_path("gold", "daily_sales")
    daily_sales.write.format("delta").mode("overwrite").saveAsTable(gold_table)

    row_count = daily_sales.count()

    logger.info(f"✓ Aggregated to {row_count} daily summaries")

    return {
        "task": "aggregate_daily_sales",
        "rows_written": row_count,
        "target_table": gold_table,
        "status": "success"
    }

# COMMAND ----------

# DBTITLE 1,Task 4: Aggregate Product Performance (Silver → Gold)
def task_aggregate_product_performance():
    """
    Aggregate product performance metrics (Silver → Gold).

    Metrics:
    - Total revenue per product
    - Total quantity sold
    - Transaction count
    - Average order value
    """
    logger.info("Starting product performance aggregation...")

    # Read from Silver
    silver_table = config.get_table_path("silver", "sales_transactions")
    df = spark.table(silver_table)

    # Aggregate by product
    product_performance = df.groupBy("product_id") \
        .agg(
            expr("sum(amount) as total_revenue"),
            expr("sum(quantity) as total_quantity"),
            expr("count(*) as transaction_count"),
            expr("avg(amount) as avg_order_value")
        ) \
        .withColumn("gold_processed_at", current_timestamp()) \
        .orderBy(col("total_revenue").desc())

    # Write to Gold
    gold_table = config.get_table_path("gold", "product_performance")
    product_performance.write.format("delta").mode("overwrite").saveAsTable(gold_table)

    row_count = product_performance.count()

    logger.info(f"✓ Aggregated to {row_count} product summaries")

    return {
        "task": "aggregate_product_performance",
        "rows_written": row_count,
        "target_table": gold_table,
        "status": "success"
    }

# COMMAND ----------

# DBTITLE 1,Task 5: Data Quality Checks
def task_quality_checks():
    """
    Perform comprehensive data quality checks across all layers.

    Checks:
    - Schema validation
    - Completeness checks
    - Data quality scores
    """
    logger.info("Starting data quality checks...")

    quality_results = {}

    # Check Silver layer
    silver_table = config.get_table_path("silver", "sales_transactions")
    silver_df = spark.table(silver_table)

    checker = DataQualityCheck(silver_df)
    report = checker.run_all_checks()

    quality_results["silver_layer"] = {
        "table": silver_table,
        "quality_score": report["quality_score"],
        "completeness_score": report["completeness_score"],
        "duplicate_percentage": report["duplicates"]["duplicate_percentage"],
    }

    # Check Gold layer - daily sales
    gold_daily = config.get_table_path("gold", "daily_sales")
    gold_df = spark.table(gold_daily)

    checker_gold = DataQualityCheck(gold_df)
    report_gold = checker_gold.run_all_checks()

    quality_results["gold_daily_sales"] = {
        "table": gold_daily,
        "quality_score": report_gold["quality_score"],
        "completeness_score": report_gold["completeness_score"],
    }

    # Log results
    logger.info(f"Silver quality score: {quality_results['silver_layer']['quality_score']:.2%}")
    logger.info(f"Gold quality score: {quality_results['gold_daily_sales']['quality_score']:.2%}")

    return {
        "task": "quality_checks",
        "results": quality_results,
        "status": "success"
    }

# COMMAND ----------

# DBTITLE 1,Task 6: Optimize Delta Tables
def task_optimize_tables():
    """
    Optimize all Delta Lake tables for performance.

    Operations:
    - OPTIMIZE: Compact small files
    - Z-ORDER: Co-locate related data
    """
    logger.info("Starting table optimization...")

    tables_to_optimize = [
        (config.get_table_path("bronze", "sales_transactions"), None),
        (config.get_table_path("silver", "sales_transactions"), ["transaction_date"]),
        (config.get_table_path("gold", "daily_sales"), ["transaction_date"]),
        (config.get_table_path("gold", "product_performance"), ["total_revenue"]),
    ]

    optimized_count = 0

    for table_path, zorder_cols in tables_to_optimize:
        try:
            optimize_table(table_path, zorder_columns=zorder_cols)
            optimized_count += 1
            logger.info(f"✓ Optimized {table_path}")
        except Exception as e:
            logger.warning(f"Could not optimize {table_path}: {e}")

    return {
        "task": "optimize_tables",
        "tables_optimized": optimized_count,
        "status": "success"
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Production Pipeline
# MAGIC
# MAGIC Assemble all tasks into a DAG-based pipeline with proper dependencies.

# COMMAND ----------

# DBTITLE 1,Create Pipeline and Add Tasks
# Create pipeline
ecommerce_pipeline = Pipeline(
    name="ecommerce_analytics_pipeline",
    config=config
)

# Add tasks with dependencies
ecommerce_pipeline.add_task(
    Task(
        name="ingest_sales",
        function=task_ingest_sales,
        retry_attempts=2
    )
)

ecommerce_pipeline.add_task(
    Task(
        name="clean_sales",
        function=task_clean_sales,
        dependencies=["ingest_sales"],
        retry_attempts=2
    )
)

ecommerce_pipeline.add_task(
    Task(
        name="aggregate_daily_sales",
        function=task_aggregate_daily_sales,
        dependencies=["clean_sales"]
    )
)

ecommerce_pipeline.add_task(
    Task(
        name="aggregate_product_performance",
        function=task_aggregate_product_performance,
        dependencies=["clean_sales"]
    )
)

ecommerce_pipeline.add_task(
    Task(
        name="quality_checks",
        function=task_quality_checks,
        dependencies=["aggregate_daily_sales", "aggregate_product_performance"]
    )
)

ecommerce_pipeline.add_task(
    Task(
        name="optimize_tables",
        function=task_optimize_tables,
        dependencies=["quality_checks"]
    )
)

print("✓ Pipeline configured successfully")

# COMMAND ----------

# DBTITLE 1,Validate Pipeline Dependencies
# Validate pipeline structure
try:
    ecommerce_pipeline.validate_dependencies()
    print("✓ Pipeline dependencies are valid")
except ValueError as e:
    print(f"✗ Pipeline validation failed: {e}")
    raise

# Show execution plan
execution_order = ecommerce_pipeline.get_execution_order()
print("\n📋 Execution Plan:")
for stage_num, tasks in enumerate(execution_order, 1):
    print(f"  Stage {stage_num}: {tasks}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline
# MAGIC
# MAGIC Run the complete production pipeline.

# COMMAND ----------

# DBTITLE 1,Run Pipeline
# Execute pipeline
print("🚀 Starting pipeline execution...\n")

results = ecommerce_pipeline.run()

# Print summary
ecommerce_pipeline.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Results
# MAGIC
# MAGIC Examine the data created by the pipeline.

# COMMAND ----------

# DBTITLE 1,Silver Layer: Cleaned Sales
silver_table = config.get_table_path("silver", "sales_transactions")
silver_df = spark.table(silver_table)

print(f"Silver Layer: {silver_table}")
print(f"Row count: {silver_df.count()}")
print("\nSample data:")
display(silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Layer: Daily Sales Summary
gold_daily = config.get_table_path("gold", "daily_sales")
daily_df = spark.table(gold_daily)

print(f"Gold Layer: {gold_daily}")
print(f"Row count: {daily_df.count()}")
print("\nDaily Sales Metrics:")
display(daily_df.orderBy("transaction_date"))

# COMMAND ----------

# DBTITLE 1,Gold Layer: Product Performance
gold_products = config.get_table_path("gold", "product_performance")
products_df = spark.table(gold_products)

print(f"Gold Layer: {gold_products}")
print(f"Row count: {products_df.count()}")
print("\nTop 10 Products by Revenue:")
display(products_df.orderBy(col("total_revenue").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Monitoring
# MAGIC
# MAGIC Extract execution metrics for monitoring and alerting.

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Metrics
results = ecommerce_pipeline.get_results()

print("📊 Pipeline Execution Metrics\n")
print(f"Pipeline: {results['name']}")
print(f"Status: {results['status']}")
print(f"Duration: {results.get('end_time', 'N/A')}")
print(f"\nTask Summary:")
print(f"  Completed: {results['tasks_completed']}")
print(f"  Failed: {results['tasks_failed']}")
print(f"  Skipped: {results['tasks_skipped']}")

print("\n📈 Task Details:")
for task_name, task_meta in results['tasks'].items():
    status = task_meta['status']
    duration = task_meta.get('duration_seconds', 0)
    print(f"  {task_name}: {status} ({duration:.2f}s)")

    # Show task-specific results
    if 'result' in task_meta and task_meta['result']:
        result = task_meta['result']
        if isinstance(result, dict):
            for key, value in result.items():
                if key != 'status' and key != 'task':
                    print(f"    - {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Deployment
# MAGIC
# MAGIC ### Converting to Databricks Job
# MAGIC
# MAGIC To schedule this pipeline in production:
# MAGIC
# MAGIC 1. **Build wheel package**:
# MAGIC    ```bash
# MAGIC    poetry build
# MAGIC    ```
# MAGIC
# MAGIC 2. **Upload to Unity Catalog Volumes**:
# MAGIC    ```python
# MAGIC    dbutils.fs.cp(
# MAGIC        "file:/path/to/databricks_infra-0.1.0-py3-none-any.whl",
# MAGIC        "dbfs:/Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl"
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 3. **Create Databricks Job**:
# MAGIC    - Type: Notebook Job
# MAGIC    - Notebook: This notebook
# MAGIC    - Library: `/Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl`
# MAGIC    - Schedule: Daily, hourly, or triggered
# MAGIC    - Cluster: Use shared cluster or new cluster
# MAGIC
# MAGIC 4. **Add monitoring**:
# MAGIC    - Email alerts on failure
# MAGIC    - Slack notifications
# MAGIC    - Custom metrics to monitoring system

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC ✓ **Complete Production Pipeline**
# MAGIC - Multi-stage data processing (Bronze → Silver → Gold)
# MAGIC - Parallel task execution where possible
# MAGIC - Automatic dependency management
# MAGIC - Built-in retry logic
# MAGIC
# MAGIC ✓ **Data Quality**
# MAGIC - Automated quality checks
# MAGIC - Validation at each layer
# MAGIC - Quality score tracking
# MAGIC
# MAGIC ✓ **Performance Optimization**
# MAGIC - Delta Lake optimization
# MAGIC - Z-ordering for query performance
# MAGIC - Efficient data layout
# MAGIC
# MAGIC ✓ **Observability**
# MAGIC - Comprehensive logging
# MAGIC - Execution metrics
# MAGIC - Pipeline monitoring
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Framework Benefits**: Reusable components reduce development time
# MAGIC 2. **Production Ready**: Built-in best practices for reliability
# MAGIC 3. **Maintainable**: Clear structure and separation of concerns
# MAGIC 4. **Scalable**: Pattern works for simple to complex pipelines
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Customize this pipeline for your data sources
# MAGIC - Add more advanced transformations
# MAGIC - Implement custom quality checks
# MAGIC - Deploy to production with Databricks Jobs
# MAGIC - Build monitoring dashboards