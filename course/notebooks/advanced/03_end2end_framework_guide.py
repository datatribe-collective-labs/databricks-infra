# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Data Engineering Framework Guide
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC The `end2end` framework is a comprehensive Python package that consolidates all the patterns you've learned in Weeks 1-5 into a reusable, production-ready framework for building data pipelines on Databricks.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - How to use the end2end framework for production pipelines
# MAGIC - Configuration management with Unity Catalog
# MAGIC - Multi-source data ingestion patterns
# MAGIC - Medallion architecture transformations
# MAGIC - Data quality validation
# MAGIC - Pipeline orchestration
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Completed Weeks 1-5 of the course
# MAGIC - Understanding of medallion architecture
# MAGIC - Familiarity with Unity Catalog
# MAGIC - Basic Python package usage
# MAGIC
# MAGIC ### Framework Benefits
# MAGIC
# MAGIC | Benefit | Description |
# MAGIC |---------|-------------|
# MAGIC | **Reusability** | Write once, use in all pipelines |
# MAGIC | **Consistency** | Standardized patterns across projects |
# MAGIC | **Maintainability** | Centralized logic, easier updates |
# MAGIC | **Production-Ready** | Built-in error handling, logging, retry logic |
# MAGIC | **Educational** | Well-documented code demonstrating best practices |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Installation and Setup
# MAGIC
# MAGIC ### Installing the Framework
# MAGIC
# MAGIC The `end2end` framework is part of the course infrastructure. For production use on Premium Edition:
# MAGIC
# MAGIC ```bash
# MAGIC # Build wheel package
# MAGIC cd /path/to/databricks-infra-org
# MAGIC poetry build
# MAGIC
# MAGIC # Upload to Unity Catalog Volumes
# MAGIC dbfs cp dist/databricks_infra-0.1.0-py3-none-any.whl \
# MAGIC   /Volumes/databricks_course/shared/packages/
# MAGIC
# MAGIC # Install in notebook
# MAGIC %pip install /Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC **Note for Free Edition:** Import directly from source (see code below)

# COMMAND ----------

# For development/Free Edition: Add src to path
import sys
import os

# Add the src directory to Python path
# Adjust path based on your notebook location
course_root = os.path.abspath("../../..")
src_path = os.path.join(course_root, "src")

if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(f"Added to Python path: {src_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Framework Components
# MAGIC
# MAGIC The `end2end` framework has six main components:
# MAGIC
# MAGIC 1. **Config** - Configuration management
# MAGIC 2. **Ingestion** - Multi-source data ingestion
# MAGIC 3. **Transformations** - Bronze → Silver → Gold
# MAGIC 4. **Quality** - Data validation and quality checks
# MAGIC 5. **Pipeline** - Workflow orchestration
# MAGIC 6. **Utils** - Logging, Spark utilities

# COMMAND ----------

# Import framework components
from end2end import (
    # Configuration
    PipelineConfig,
    create_user_config,

    # Ingestion
    FileIngestion,
    APIIngestion,
    DatabaseIngestion,

    # Transformations
    BronzeToSilver,
    SilverToGold,

    # Quality
    SchemaValidator,
    DataQualityCheck,
    Expectation,

    # Pipeline
    Pipeline,
    Task,

    # Utilities
    get_logger,
    optimize_table,
)

logger = get_logger(__name__)
print("✓ Successfully imported end2end framework")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Configuration Management
# MAGIC
# MAGIC ### Pattern from Week 1: Unity Catalog Three-Level Namespace
# MAGIC
# MAGIC Configuration handles the `catalog.schema.table` pattern automatically.

# COMMAND ----------

# DBTITLE 1,Basic Configuration
# Create configuration for your user schema
config = PipelineConfig(
    catalog="databricks_course",
    source_schema="shared",  # Read from shared schemas
    target_schema="chanukya_pekala",  # Write to your personal schema (update this!)
    environment="dev",
)

# Display configuration
print(f"Configuration: {config}")
print(f"\nSchema path: {config.get_schema_path()}")
print(f"Bronze table: {config.get_table_path('bronze', 'sales')}")
print(f"Silver table: {config.get_table_path('silver', 'sales')}")
print(f"Gold table: {config.get_table_path('gold', 'daily_summary')}")

# COMMAND ----------

# DBTITLE 1,User-Specific Configuration
# Alternative: Create config from user email (mimics user_schema_setup.py pattern)
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_config = create_user_config(user_email)

print(f"User email: {user_email}")
print(f"User schema: {user_config.source_schema}")
print(f"User config: {user_config}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Data Ingestion
# MAGIC
# MAGIC ### Pattern from Week 2: Multi-Source Ingestion

# COMMAND ----------

# DBTITLE 1,File Ingestion Example
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define explicit schema (Week 2 best practice)
sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("transaction_date", TimestampType(), False),
])

# Create file ingestion task
# Note: Update path to your actual data location
file_ingestion = FileIngestion(
    config=config,
    source_path="/databricks-datasets/retail-org/sales_orders/",  # Example path
    file_format="parquet",
    schema=sales_schema,
)

# Execute ingestion
# result = file_ingestion.execute("sales_transactions")
# print(f"✓ Ingested {result['rows_read']} rows to {result['target_table']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestion with Validation

# COMMAND ----------

# DBTITLE 1,Custom Validation Example
# You can add custom validation to ingestion
class ValidatedFileIngestion(FileIngestion):
    def validate_data(self, df):
        # Call parent validation
        df = super().validate_data(df)

        # Add custom validation
        # 1. Check for negative amounts
        negative_amounts = df.filter(df["amount"] < 0).count()
        if negative_amounts > 0:
            logger.warning(f"Found {negative_amounts} transactions with negative amounts")

        # 2. Check date range
        from pyspark.sql.functions import min, max
        date_stats = df.select(
            min("transaction_date").alias("min_date"),
            max("transaction_date").alias("max_date")
        ).collect()[0]

        logger.info(f"Date range: {date_stats['min_date']} to {date_stats['max_date']}")

        return df

# Use custom ingestion
# custom_ingestion = ValidatedFileIngestion(
#     config=config,
#     source_path="/path/to/data",
#     file_format="csv",
#     schema=sales_schema,
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Transformations
# MAGIC
# MAGIC ### Pattern from Week 3: Medallion Architecture

# COMMAND ----------

# DBTITLE 1,Bronze to Silver Transformation
# Create Bronze → Silver transformation
bronze_to_silver = BronzeToSilver(
    config=config,
    source_table="sales_transactions",
    target_table="sales_transactions",  # Same name, different layer
    remove_duplicates=True,
    required_columns=["transaction_id", "customer_id", "amount"],
    transformations={
        "customer_id": "upper(trim(customer_id))",  # Standardize customer ID
        "product_id": "upper(trim(product_id))",    # Standardize product ID
    },
)

# Execute transformation
# result = bronze_to_silver.execute("bronze", "silver")
# print(f"✓ Transformed {result['rows_read']} → {result['rows_written']} rows")

# COMMAND ----------

# DBTITLE 1,Silver to Gold Aggregation
# Create Silver → Gold aggregation
silver_to_gold = SilverToGold(
    config=config,
    source_table="sales_transactions",
    target_table="daily_sales_summary",
    group_by_columns=["transaction_date", "product_id"],
    aggregations={
        "amount": ["sum", "avg", "count"],
        "quantity": ["sum"],
    },
)

# Execute aggregation
# result = silver_to_gold.execute("silver", "gold")
# print(f"✓ Aggregated {result['rows_read']} → {result['rows_written']} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Data Quality
# MAGIC
# MAGIC ### Pattern: Production-Grade Validation

# COMMAND ----------

# DBTITLE 1,Schema Validation
# Define expected schema
expected_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False),
])

# Create validator
validator = SchemaValidator(expected_schema)

# Validate a DataFrame
# df = spark.table("databricks_course.chanukya_pekala.bronze_sales_transactions")
# is_valid = validator.validate(df)
# print(f"Schema validation: {'✓ PASSED' if is_valid else '✗ FAILED'}")

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# Perform comprehensive quality checks
# df = spark.table("databricks_course.chanukya_pekala.silver_sales_transactions")
# checker = DataQualityCheck(df)
# checker.print_report()

# COMMAND ----------

# DBTITLE 1,Expectation-Based Validation
# Create expectations (Great Expectations pattern)
amount_expectation = Expectation("amount")
amount_expectation.expect_column_to_exist() \
                  .expect_column_values_to_not_be_null() \
                  .expect_column_values_to_be_between(0, 1000000)

# Validate expectations
# df = spark.table("databricks_course.chanukya_pekala.silver_sales_transactions")
# result = amount_expectation.validate(df)
# print(f"Expectations: {result['passed']}/{result['total_expectations']} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Pipeline Orchestration
# MAGIC
# MAGIC ### Pattern from Week 4 & 5: DAG-Based Workflows

# COMMAND ----------

# DBTITLE 1,Simple Pipeline Example
def ingest_data():
    """Ingestion task function."""
    logger.info("Ingesting sales data...")
    # Ingestion logic here
    return {"rows": 1000, "status": "success"}

def transform_to_silver():
    """Transformation task function."""
    logger.info("Transforming to Silver layer...")
    # Transformation logic here
    return {"rows": 950, "status": "success"}

def aggregate_to_gold():
    """Aggregation task function."""
    logger.info("Aggregating to Gold layer...")
    # Aggregation logic here
    return {"rows": 100, "status": "success"}

# Create pipeline
pipeline = Pipeline("sales_pipeline", config)

# Add tasks with dependencies
pipeline.add_task(Task("ingest", ingest_data)) \
        .add_task(Task("transform", transform_to_silver, dependencies=["ingest"])) \
        .add_task(Task("aggregate", aggregate_to_gold, dependencies=["transform"]))

# Execute pipeline
results = pipeline.run(dry_run=True)  # dry_run=True for validation only
pipeline.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Complete Example
# MAGIC
# MAGIC ### Real-World Pipeline: Ingest → Transform → Aggregate

# COMMAND ----------

# DBTITLE 1,Define Pipeline Tasks with Framework Components
def task_ingest_sales():
    """Ingest sales data to Bronze layer."""
    ingestion = FileIngestion(
        config=config,
        source_path="/databricks-datasets/retail-org/sales_orders/",
        file_format="parquet",
    )
    return ingestion.execute("sales_transactions")

def task_clean_sales():
    """Clean and validate sales data (Bronze → Silver)."""
    transformation = BronzeToSilver(
        config=config,
        source_table="sales_transactions",
        remove_duplicates=True,
        required_columns=["customer_id", "amount"],
    )
    return transformation.execute("bronze", "silver")

def task_aggregate_sales():
    """Aggregate sales data (Silver → Gold)."""
    aggregation = SilverToGold(
        config=config,
        source_table="sales_transactions",
        target_table="daily_sales",
        group_by_columns=["transaction_date"],
        aggregations={"amount": ["sum", "avg", "count"]},
    )
    return aggregation.execute("silver", "gold")

def task_optimize_tables():
    """Optimize Delta Lake tables."""
    tables = [
        config.get_table_path("bronze", "sales_transactions"),
        config.get_table_path("silver", "sales_transactions"),
        config.get_table_path("gold", "daily_sales"),
    ]
    for table in tables:
        try:
            optimize_table(table)
            logger.info(f"✓ Optimized {table}")
        except Exception as e:
            logger.warning(f"Could not optimize {table}: {e}")

    return {"tables_optimized": len(tables)}

# COMMAND ----------

# DBTITLE 1,Build and Execute Complete Pipeline
# Create end-to-end pipeline
complete_pipeline = Pipeline("complete_sales_pipeline", config)

# Add all tasks with dependencies
complete_pipeline.add_task(Task("ingest", task_ingest_sales, retry_attempts=2))
complete_pipeline.add_task(Task("clean", task_clean_sales, dependencies=["ingest"]))
complete_pipeline.add_task(Task("aggregate", task_aggregate_sales, dependencies=["clean"]))
complete_pipeline.add_task(Task("optimize", task_optimize_tables, dependencies=["aggregate"]))

# Validate pipeline structure
try:
    complete_pipeline.validate_dependencies()
    print("✓ Pipeline dependencies are valid")
except ValueError as e:
    print(f"✗ Pipeline validation failed: {e}")

# Show execution plan
execution_order = complete_pipeline.get_execution_order()
print("\nExecution Plan:")
for stage_num, tasks in enumerate(execution_order, 1):
    print(f"  Stage {stage_num}: {tasks}")

# Execute pipeline (uncomment to run)
# results = complete_pipeline.run()
# complete_pipeline.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Best Practices
# MAGIC
# MAGIC ### 1. Configuration Management
# MAGIC - Always use `PipelineConfig` for Unity Catalog namespaces
# MAGIC - Separate dev/staging/prod configurations
# MAGIC - Store sensitive credentials in Databricks secrets
# MAGIC
# MAGIC ### 2. Data Ingestion
# MAGIC - Define explicit schemas (avoid schema inference)
# MAGIC - Implement retry logic for external sources
# MAGIC - Validate data after ingestion
# MAGIC - Use appropriate file formats (Parquet for analytics)
# MAGIC
# MAGIC ### 3. Transformations
# MAGIC - Follow medallion architecture (Bronze → Silver → Gold)
# MAGIC - Keep transformations idempotent
# MAGIC - Add metadata columns (processed_at, etc.)
# MAGIC - Document business logic in code
# MAGIC
# MAGIC ### 4. Data Quality
# MAGIC - Validate schemas before transformations
# MAGIC - Implement expectation-based testing
# MAGIC - Monitor quality metrics over time
# MAGIC - Set up alerts for quality failures
# MAGIC
# MAGIC ### 5. Pipeline Orchestration
# MAGIC - Define clear task dependencies
# MAGIC - Implement retry logic for transient failures
# MAGIC - Log execution metrics
# MAGIC - Use dry_run for pipeline validation
# MAGIC
# MAGIC ### 6. Performance
# MAGIC - Optimize Delta tables regularly
# MAGIC - Use partitioning for large tables
# MAGIC - Cache frequently accessed data
# MAGIC - Monitor query performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Deployment to Production
# MAGIC
# MAGIC ### Building the Wheel Package
# MAGIC
# MAGIC ```bash
# MAGIC # In your local environment
# MAGIC cd /path/to/databricks-infra-org
# MAGIC
# MAGIC # Build wheel
# MAGIC poetry build
# MAGIC
# MAGIC # Output: dist/databricks_infra-0.1.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC ### Uploading to Unity Catalog Volumes
# MAGIC
# MAGIC ```python
# MAGIC # In Databricks notebook
# MAGIC dbutils.fs.cp(
# MAGIC     "file:/Workspace/Users/your.email@example.com/databricks_infra-0.1.0-py3-none-any.whl",
# MAGIC     "dbfs:/Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Installing in Notebooks
# MAGIC
# MAGIC ```python
# MAGIC %pip install /Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC ### Using in Databricks Jobs
# MAGIC
# MAGIC In job configuration, add library:
# MAGIC - Type: Python Wheel
# MAGIC - Path: `/Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've learned how to use the `end2end` framework for production data pipelines:
# MAGIC
# MAGIC ✓ **Configuration** - Unity Catalog namespace management
# MAGIC ✓ **Ingestion** - Multi-source data ingestion with validation
# MAGIC ✓ **Transformations** - Medallion architecture (Bronze → Silver → Gold)
# MAGIC ✓ **Quality** - Schema validation and expectations
# MAGIC ✓ **Pipeline** - DAG-based workflow orchestration
# MAGIC ✓ **Utilities** - Logging, optimization, and helpers
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Practice**: Try notebook `04_end2end_production_pipeline.py` for a real-world example
# MAGIC 2. **Customize**: Extend the framework with your own ingestion/transformation classes
# MAGIC 3. **Deploy**: Build wheel packages for your production pipelines
# MAGIC 4. **Scale**: Use the framework across multiple projects for consistency
# MAGIC
# MAGIC ### Resources
# MAGIC
# MAGIC - Framework source code: `src/end2end/`
# MAGIC - Documentation: `docs/END2END_FRAMEWORK_GUIDE.md`
# MAGIC - Course notebooks: Weeks 1-5 for pattern details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercises
# MAGIC
# MAGIC Try these exercises to solidify your understanding:
# MAGIC
# MAGIC 1. **Custom Ingestion**: Create a custom ingestion class for a new data source
# MAGIC 2. **Complex Transformation**: Build a Bronze → Silver transformation with business rules
# MAGIC 3. **Quality Suite**: Create a comprehensive quality check suite for your data
# MAGIC 4. **Production Pipeline**: Build a complete pipeline with all components
# MAGIC 5. **Monitoring**: Add custom logging and metrics to track pipeline health