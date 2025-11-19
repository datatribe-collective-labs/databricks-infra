# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Deep Dive - Week 1
# MAGIC
# MAGIC Unity Catalog is Databricks' unified governance solution for data and AI assets. This notebook provides comprehensive coverage of Unity Catalog architecture, features, and best practices.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand Unity Catalog architecture and hierarchy
# MAGIC - Learn about data governance and lineage tracking
# MAGIC - Master access control and permission management
# MAGIC - Explore data discovery and metadata management
# MAGIC - Implement data classification and tagging strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Architecture
# MAGIC
# MAGIC Unity Catalog provides a three-level namespace for organizing data assets:
# MAGIC
# MAGIC ```
# MAGIC Account
# MAGIC ├── Metastore (regional)
# MAGIC │   ├── Catalog_1
# MAGIC │   │   ├── Schema_1 (Database)
# MAGIC │   │   │   ├── Table_1
# MAGIC │   │   │   ├── View_1
# MAGIC │   │   │   ├── Function_1
# MAGIC │   │   │   └── Volume_1
# MAGIC │   │   ├── Schema_2
# MAGIC │   │   └── Schema_N
# MAGIC │   ├── Catalog_2
# MAGIC │   └── Catalog_N
# MAGIC │   
# MAGIC └── Workspaces (attached to metastore)
# MAGIC     ├── Workspace_1
# MAGIC     ├── Workspace_2
# MAGIC     └── Workspace_N
# MAGIC ```
# MAGIC
# MAGIC ### Key Components
# MAGIC
# MAGIC 1. **Account**: Top-level Databricks organization
# MAGIC 2. **Metastore**: Regional metadata repository
# MAGIC 3. **Catalog**: Highest level of data organization
# MAGIC 4. **Schema**: Logical grouping of data assets (equivalent to database)
# MAGIC 5. **Objects**: Tables, views, functions, volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Environment Exploration
# MAGIC
# MAGIC Let's explore the Unity Catalog structure in our current environment:

# COMMAND ----------

# Explore current Unity Catalog configuration
print("=== Unity Catalog Environment ===")

try:
    # Get current catalog information
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    current_schema = spark.sql("SELECT current_database()").collect()[0][0]
    
    print(f"Current Catalog: {current_catalog}")
    print(f"Current Schema: {current_schema}")
    print(f"Full Current Path: {current_catalog}.{current_schema}")
    
except Exception as e:
    print(f"Unity Catalog information not available: {e}")
    print("This might be a legacy Hive metastore environment")

# COMMAND ----------

# List available catalogs
print("=== Available Catalogs ===")

try:
    catalogs_df = spark.sql("SHOW CATALOGS")
    catalogs_df.show(truncate=False)
    
    # Store catalog names for later use
    catalog_names = [row['catalog'] for row in catalogs_df.collect()]
    print(f"\nFound {len(catalog_names)} catalogs: {catalog_names}")
    
except Exception as e:
    print(f"Could not retrieve catalogs: {e}")
    catalog_names = []

# COMMAND ----------

# Explore schemas within each catalog
print("=== Catalog and Schema Structure ===")

for catalog in catalog_names[:3]:  # Limit to first 3 catalogs to avoid overwhelming output
    try:
        print(f"\n--- Catalog: {catalog} ---")
        
        # List schemas in this catalog
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
        schemas = [row['databaseName'] for row in schemas_df.collect()]
        
        print(f"Schemas ({len(schemas)}): {schemas}")
        
        # Show tables in first schema of each catalog
        if schemas:
            first_schema = schemas[0]
            print(f"\nTables in {catalog}.{first_schema}:")
            try:
                tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{first_schema}")
                table_count = tables_df.count()
                print(f"Found {table_count} tables")
                if table_count > 0:
                    tables_df.select("tableName", "isTemporary").show(5, truncate=False)
            except Exception as e:
                print(f"Could not list tables: {e}")
                
    except Exception as e:
        print(f"Error exploring catalog {catalog}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Permissions Model
# MAGIC
# MAGIC Unity Catalog implements a hierarchical permissions model:
# MAGIC
# MAGIC ### Permission Hierarchy
# MAGIC
# MAGIC ```
# MAGIC Account Admin
# MAGIC ├── Metastore Admin
# MAGIC │   ├── Catalog Owner/Admin
# MAGIC │   │   ├── Schema Owner/Admin
# MAGIC │   │   │   ├── Table/View Owner
# MAGIC │   │   │   ├── Function Owner
# MAGIC │   │   │   └── Volume Owner
# MAGIC │   │   └── Data Reader/Writer
# MAGIC │   └── Data Reader/Writer
# MAGIC └── Workspace Admin/User
# MAGIC ```
# MAGIC
# MAGIC ### Key Privileges
# MAGIC
# MAGIC | Level | Privileges |
# MAGIC |-------|------------|
# MAGIC | **Catalog** | USE CATALOG, CREATE SCHEMA, BROWSE |
# MAGIC | **Schema** | USE SCHEMA, CREATE TABLE, CREATE VIEW, CREATE FUNCTION |
# MAGIC | **Table** | SELECT, INSERT, UPDATE, DELETE, MODIFY |
# MAGIC | **View** | SELECT, BROWSE |
# MAGIC | **Function** | EXECUTE |
# MAGIC | **Volume** | READ VOLUME, WRITE VOLUME |

# COMMAND ----------

# Demonstrate permission checking (if available)
print("=== Permission Exploration ===")

try:
    # Check what we can see in the current context
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    print(f"Current User: {current_user}")
    
    # Try to show grants on current catalog
    if catalog_names:
        first_catalog = catalog_names[0]
        print(f"\nAttempting to show grants for catalog: {first_catalog}")
        try:
            grants_df = spark.sql(f"SHOW GRANTS ON CATALOG {first_catalog}")
            grants_df.show(truncate=False)
        except Exception as e:
            print(f"Cannot show grants (requires admin privileges): {e}")
    
except Exception as e:
    print(f"User information not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lineage and Metadata
# MAGIC
# MAGIC Unity Catalog automatically captures data lineage and metadata:
# MAGIC
# MAGIC ### Automatic Lineage Tracking
# MAGIC
# MAGIC - **Table-to-Table**: Tracks data transformations between tables
# MAGIC - **Column-Level**: Traces individual column dependencies
# MAGIC - **Notebook Integration**: Links code changes to data changes
# MAGIC - **Job Lineage**: Connects scheduled jobs to data assets
# MAGIC
# MAGIC ### Metadata Captured
# MAGIC
# MAGIC 1. **Schema Information**: Column names, types, constraints
# MAGIC 2. **Statistics**: Row counts, data freshness, quality metrics
# MAGIC 3. **Access Patterns**: Usage frequency, popular queries
# MAGIC 4. **Data Quality**: Nullability, uniqueness, distributions

# COMMAND ----------

# Create a sample table to demonstrate metadata features
print("=== Metadata and Lineage Demonstration ===")

# Create sample business data
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql.functions import col, current_date, lit
from decimal import Decimal
from datetime import date

# Define schema for a sales table
sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("sale_date", DateType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("region", StringType(), False),
    StructField("sales_rep", StringType(), True)
])

# Create sample sales data
sales_data = [
    (1, 101, 501, date(2024, 1, 15), 2, Decimal('29.99'), 'North', 'Alice Johnson'),
    (2, 102, 502, date(2024, 1, 16), 1, Decimal('149.99'), 'South', 'Bob Smith'),
    (3, 103, 501, date(2024, 1, 17), 3, Decimal('29.99'), 'East', 'Carol Davis'),
    (4, 104, 503, date(2024, 1, 18), 1, Decimal('79.99'), 'West', 'David Wilson'),
    (5, 105, 502, date(2024, 1, 19), 2, Decimal('149.99'), 'North', 'Alice Johnson')
]

# Create DataFrame
sales_df = spark.createDataFrame(sales_data, sales_schema)

print("Sample Sales Data:")
sales_df.show()

# Calculate derived metrics (this creates lineage)
sales_summary = sales_df.groupBy("region", "sales_rep") \
    .agg(
        {"quantity": "sum", "unit_price": "avg", "sale_id": "count"}
    ) \
    .withColumnRenamed("sum(quantity)", "total_quantity") \
    .withColumnRenamed("avg(unit_price)", "avg_unit_price") \
    .withColumnRenamed("count(sale_id)", "total_sales")

print("\nSales Summary (creates lineage relationship):")
sales_summary.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Classification and Tagging
# MAGIC
# MAGIC Unity Catalog supports data classification for governance and compliance:
# MAGIC
# MAGIC ### Classification Levels
# MAGIC
# MAGIC 1. **Public**: No restrictions, can be shared freely
# MAGIC 2. **Internal**: Restricted to organization members
# MAGIC 3. **Confidential**: Limited access, requires approval
# MAGIC 4. **Restricted**: Highly sensitive, strict access controls
# MAGIC
# MAGIC ### Tag Categories
# MAGIC
# MAGIC - **Data Type**: PII, Financial, Healthcare, etc.
# MAGIC - **Geography**: US, EU, Global, etc.
# MAGIC - **Compliance**: GDPR, HIPAA, SOX, etc.
# MAGIC - **Quality**: Gold, Silver, Bronze
# MAGIC - **Freshness**: Real-time, Daily, Historical

# COMMAND ----------

# Demonstrate table creation with metadata
print("=== Table Creation with Metadata ===")

# Create a temporary view with business context
sales_df.createOrReplaceTempView("temp_sales_data")

# Add business metadata through SQL comments
metadata_sql = """
CREATE OR REPLACE TEMPORARY VIEW sales_with_metadata AS
SELECT 
    sale_id,
    customer_id,
    product_id,
    sale_date,
    quantity,
    unit_price,
    (quantity * unit_price) as total_amount,
    region,
    sales_rep,
    current_date() as processed_date
FROM temp_sales_data
"""

spark.sql(metadata_sql)

# Show the enhanced data
print("Enhanced sales data with calculated fields:")
enhanced_sales = spark.sql("SELECT * FROM sales_with_metadata")
enhanced_sales.show()

print("\nSchema with business context:")
enhanced_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Discovery and Search
# MAGIC
# MAGIC Unity Catalog provides powerful data discovery capabilities:
# MAGIC
# MAGIC ### Discovery Features
# MAGIC
# MAGIC 1. **Full-Text Search**: Search across table and column names, descriptions
# MAGIC 2. **Tag-Based Search**: Find data by classification tags
# MAGIC 3. **Lineage Search**: Discover upstream and downstream dependencies
# MAGIC 4. **Usage Analytics**: Find popular and frequently accessed data
# MAGIC
# MAGIC ### Search Best Practices
# MAGIC
# MAGIC - **Descriptive Naming**: Use clear, business-meaningful names
# MAGIC - **Rich Metadata**: Add detailed descriptions and tags
# MAGIC - **Consistent Taxonomy**: Establish naming conventions
# MAGIC - **Regular Maintenance**: Keep metadata current and accurate

# COMMAND ----------

# Demonstrate information schema queries for data discovery
print("=== Data Discovery Examples ===")

# Note: These queries work in Unity Catalog environments
# In legacy Hive metastore, some features may not be available

discovery_queries = [
    ("Find tables containing 'sales'", "SHOW TABLES LIKE '*sales*'"),
    ("Find columns containing 'customer'", "SELECT * FROM information_schema.columns WHERE column_name LIKE '%customer%'"),
    ("Table statistics", "DESCRIBE DETAIL temp_sales_data"),
    ("Column statistics", "DESCRIBE temp_sales_data")
]

for description, query in discovery_queries:
    print(f"\n--- {description} ---")
    try:
        result = spark.sql(query)
        if result.count() > 0:
            result.show(5, truncate=False)
        else:
            print("No results found")
    except Exception as e:
        print(f"Query not supported in this environment: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumes and External Storage
# MAGIC
# MAGIC Unity Catalog Volumes provide managed storage for non-tabular data:
# MAGIC
# MAGIC ### Volume Types
# MAGIC
# MAGIC 1. **Managed Volumes**: Databricks manages the storage lifecycle
# MAGIC 2. **External Volumes**: References external cloud storage locations
# MAGIC
# MAGIC ### Use Cases
# MAGIC
# MAGIC - **Model Artifacts**: ML models, checkpoints, configurations
# MAGIC - **Data Files**: CSV, JSON, images, documents
# MAGIC - **Notebooks**: Shared notebook storage
# MAGIC - **Libraries**: Custom packages and dependencies

# COMMAND ----------

%run ../utils/user_schema_setup.py

# COMMAND ----------

# Demonstrate file operations and volume concepts
print("=== File Operations and Volume Concepts ===")

# Show current working directory and available mounts
try:
    import os
    print(f"Current working directory: {os.getcwd()}")
    
    # List available file systems (if dbutils is available)
    print("\nAttempting to list file systems:")
    
    # Try to access DBFS. This is not allowed in the Free Edition of Databricks, and query will fail.
    try:
        dbutils.fs.ls("/")[:5]  # Show first 5 entries
        print("DBFS root directory accessible")
    except Exception as e:
        print(f"DBFS access limited: {e}")

    # The Free Edition of Databricks doesn't allow access on DBFS system with dbutils.fs.ls. Normally that is the correct method,\
    # but we can print same information via Python os module as follows:
    print("\nListing file systems using the 'os' module:")

    # The DBFS root (/) is mounted at /dbfs/ on the driver node's local filesystem
    dbfs_mount_path = "/dbfs/"

    try:
        # List the contents of the /dbfs/ directory
        contents = os.listdir(dbfs_mount_path)
        print(f"DBFS root directory visible in {dbfs_mount_path}")
        print(f"First 5 entries: {contents[:5]}")
    except FileNotFoundError:
        print(f"Error: Directory not found: {dbfs_mount_path}")
    except Exception as e:
        print(f"An error occurred while accessing {dbfs_mount_path}: {e}")
        
        
except Exception as e:
    print(f"File system exploration limited: {e}")

# Demonstrate saving data in different formats
print("\nSaving data in various formats for volume storage:")

# Save as Parquet (optimized for analytics)
try:
    sales_df.write.mode("overwrite").parquet(f"{VOLUME_PATH}sales_data")
    print("✓ Saved as Parquet format")
except Exception as e:
    print(f"Parquet save failed: {e}")

# Save as Delta (with ACID properties)
try:
    sales_df.write.mode("overwrite").format("delta").save(f"{VOLUME_PATH}sales_data_delta")
    print("✓ Saved as Delta format")
except Exception as e:
    print(f"Delta save failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for Unity Catalog
# MAGIC
# MAGIC ### Naming Conventions
# MAGIC
# MAGIC ```sql
# MAGIC -- Recommended naming pattern
# MAGIC <environment>_<domain>_<data_type>_<granularity>
# MAGIC
# MAGIC Examples:
# MAGIC - prod_sales_transactions_daily
# MAGIC - dev_marketing_campaigns_monthly
# MAGIC - staging_finance_reports_quarterly
# MAGIC ```
# MAGIC
# MAGIC ### Catalog Organization Strategy
# MAGIC
# MAGIC 1. **By Environment**: prod, staging, dev catalogs
# MAGIC 2. **By Domain**: sales, marketing, finance catalogs  
# MAGIC 3. **By Data Layer**: bronze, silver, gold catalogs
# MAGIC 4. **Hybrid Approach**: Combine strategies as needed
# MAGIC
# MAGIC ### Schema Organization
# MAGIC
# MAGIC - **Raw Data**: Landing zone for ingested data
# MAGIC - **Processed**: Cleaned and validated data
# MAGIC - **Analytics**: Business-ready datasets
# MAGIC - **Archive**: Historical data retention

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance and Compliance Features
# MAGIC
# MAGIC ### Data Lineage Benefits
# MAGIC
# MAGIC 1. **Impact Analysis**: Understand downstream effects of changes
# MAGIC 2. **Root Cause Analysis**: Trace data quality issues to source
# MAGIC 3. **Compliance Reporting**: Document data processing for audits
# MAGIC 4. **Change Management**: Assess risks before modifications
# MAGIC
# MAGIC ### Audit and Monitoring
# MAGIC
# MAGIC - **Access Logs**: Track who accessed what data when
# MAGIC - **Query History**: Monitor data usage patterns
# MAGIC - **Permission Changes**: Audit access control modifications
# MAGIC - **Data Quality Metrics**: Monitor data health over time

# COMMAND ----------

# Demonstrate audit and monitoring concepts
print("=== Audit and Monitoring Demonstration ===")

# Show query execution information
print("Query execution metadata:")

# Demonstrate query performance monitoring
from pyspark.sql.functions import count, sum as spark_sum, avg, max as spark_max

# Create a query that will generate metrics
performance_query = sales_df.agg(
    count("*").alias("total_records"),
    spark_sum("quantity").alias("total_quantity"),
    avg("unit_price").alias("avg_price"),
    spark_max("sale_date").alias("latest_sale")
)

print("\nQuery performance metrics:")
performance_result = performance_query.collect()[0]
print(f"- Total records processed: {performance_result['total_records']}")
print(f"- Total quantity: {performance_result['total_quantity']}")
print(f"- Average price: ${performance_result['avg_price']:.2f}")
print(f"- Latest sale date: {performance_result['latest_sale']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Other Databricks Services
# MAGIC
# MAGIC Unity Catalog integrates seamlessly with:
# MAGIC
# MAGIC ### Data Engineering
# MAGIC - **Delta Live Tables**: Managed ETL pipelines with automatic lineage
# MAGIC - **Autoloader**: Incremental data ingestion with schema evolution
# MAGIC - **Workflows**: Orchestrated data processing jobs
# MAGIC
# MAGIC ### Machine Learning
# MAGIC - **MLflow**: Model registry with Unity Catalog integration
# MAGIC - **Feature Store**: Centralized feature management
# MAGIC - **AutoML**: Automated machine learning workflows
# MAGIC
# MAGIC ### Analytics
# MAGIC - **SQL Warehouses**: High-performance analytics queries
# MAGIC - **Dashboards**: Business intelligence and reporting
# MAGIC - **Partner Integrations**: BI tools and data visualization

# COMMAND ----------

# Clean up temporary resources
spark.sql("DROP VIEW IF EXISTS temp_sales_data")
spark.sql("DROP VIEW IF EXISTS sales_with_metadata")

print("=== Unity Catalog Deep Dive Complete ===")
print("\nKey Takeaways:")
print("1. Unity Catalog provides centralized data governance")
print("2. Three-level namespace: Catalog → Schema → Objects")
print("3. Automatic lineage tracking and metadata management")
print("4. Fine-grained access control and permissions")
print("5. Integration with all Databricks services")
print("\nNext: Continue to 03_cluster_management for compute optimization!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Unity Catalog Official Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - [Best Practices Guide](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
# MAGIC - [Migration Guide](https://docs.databricks.com/data-governance/unity-catalog/migrate.html)
# MAGIC
# MAGIC ### Training Materials
# MAGIC - Databricks Academy Unity Catalog Course
# MAGIC - Unity Catalog Fundamentals Certification
# MAGIC - Data Governance with Unity Catalog Workshop