# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Fundamentals - Week 01
# MAGIC 
# MAGIC Welcome to the comprehensive Databricks fundamentals course! This notebook series provides in-depth coverage of Databricks concepts, architecture, and practical usage patterns.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of this week, you will understand:
# MAGIC - Databricks architecture and core components
# MAGIC - Unity Catalog structure and data governance
# MAGIC - Cluster management and compute optimization
# MAGIC - Databricks Runtime and Spark fundamentals
# MAGIC - Data Lakehouse technical foundation with Delta Lake (ACID & Schema)
# MAGIC 
# MAGIC ## Course Structure
# MAGIC 
# MAGIC This week consists of 6 comprehensive notebooks:
# MAGIC - **01_databricks_fundamentals** (this notebook) - Overview and architecture
# MAGIC - **02_unity_catalog_deep_dive** - Data governance and catalog structure
# MAGIC - **03_cluster_management** - Compute resources and optimization
# MAGIC - **04_spark_on_databricks** - Distributed computing fundamentals
# MAGIC - **05_delta_lake_concepts_explained** - Delta Lake: ACID Transactions and Schema Management

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Databricks?
# MAGIC 
# MAGIC Databricks is a unified analytics platform that combines:
# MAGIC - **Data Engineering**: ETL/ELT pipelines, data ingestion, transformation
# MAGIC - **Data Science**: ML experimentation, model training, MLOps
# MAGIC - **Analytics**: SQL analytics, dashboards, business intelligence
# MAGIC - **Machine Learning**: End-to-end ML lifecycle management
# MAGIC 
# MAGIC ### Key Value Propositions
# MAGIC 
# MAGIC 1. **Unified Platform**: Single platform for all data and AI workloads
# MAGIC 2. **Collaborative**: Shared workspace for data teams
# MAGIC 3. **Scalable**: Auto-scaling compute resources
# MAGIC 4. **Open**: Built on open standards (Apache Spark, Delta Lake)
# MAGIC 5. **Governed**: Enterprise-grade security and compliance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Architecture Overview
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Databricks Control Plane                 │
# MAGIC │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
# MAGIC │  │ Workspace   │  │ Unity       │  │ Job Scheduler       │  │
# MAGIC │  │ Management  │  │ Catalog     │  │ & Orchestration     │  │
# MAGIC │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Databricks Data Plane                    │
# MAGIC │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
# MAGIC │  │ Compute     │  │ Storage     │  │ Networking          │  │
# MAGIC │  │ Clusters    │  │ (DBFS,      │  │ (VPC, Security      │  │
# MAGIC │  │             │  │ Unity       │  │ Groups)             │  │
# MAGIC │  │             │  │ Catalog)    │  │                     │  │
# MAGIC │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ### Control Plane
# MAGIC - **Workspace Management**: User interface, notebooks, jobs
# MAGIC - **Unity Catalog**: Centralized data governance and metadata
# MAGIC - **Job Scheduler**: Workflow orchestration and automation
# MAGIC 
# MAGIC ### Data Plane
# MAGIC - **Compute**: Managed Spark clusters with auto-scaling
# MAGIC - **Storage**: DBFS, Unity Catalog volumes, external storage
# MAGIC - **Networking**: Secure connectivity and access controls

# COMMAND ----------

# Let's explore the current Databricks environment
print("=== Databricks Environment Information ===")

# Get Spark context information
print(f"Spark Version: {spark.version}")

# Get cluster information
import os
cluster_id = os.getenv('DATABRICKS_CLUSTER_ID', 'Not available in this environment')
print(f"Cluster ID: {cluster_id}")

# Check available Python packages
import sys
print(f"Python Version: {sys.version}")

# Display current database context
print(f"Current Database: {spark.sql('SELECT current_database()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Runtime (DBR)
# MAGIC 
# MAGIC The Databricks Runtime is a set of core components that run on Databricks clusters:
# MAGIC 
# MAGIC ### Runtime Components
# MAGIC 
# MAGIC 1. **Apache Spark**: Distributed computing engine
# MAGIC 2. **Delta Lake**: Open-source storage layer with ACID transactions
# MAGIC 3. **MLflow**: Machine learning lifecycle management
# MAGIC 4. **Pre-installed Libraries**: Optimized data science and ML libraries
# MAGIC 5. **Databricks Utilities**: Helper functions for file system operations
# MAGIC 6. **Auto-optimization**: Automatic performance tuning

# COMMAND ----------

# Explore Databricks Utilities (dbutils)
print("=== Databricks Utilities (dbutils) ===")

# File system operations
print("Available dbutils modules:")
print("- dbutils.fs: File system utilities")
print("- dbutils.widgets: Interactive widgets for notebooks")
print("- dbutils.secrets: Secure credential management")
print("- dbutils.notebook: Notebook workflow utilities")

# Example: List available file systems
print("\nAvailable file systems:")
try:
    # Note: This might not work in all environments
    file_systems = dbutils.fs.mounts()
    for mount in file_systems[:3]:  # Show first 3 mounts
        print(f"- {mount.mountPoint} -> {mount.source}")
except Exception as e:
    print(f"File system information not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Integration
# MAGIC 
# MAGIC Delta Lake is deeply integrated into Databricks and provides:
# MAGIC 
# MAGIC ### Key Features
# MAGIC 
# MAGIC 1. **ACID Transactions**: Ensures data consistency
# MAGIC 2. **Time Travel**: Query historical versions of data
# MAGIC 3. **Schema Evolution**: Safely evolve table schemas
# MAGIC 4. **Optimized Performance**: Auto-optimization and caching
# MAGIC 5. **Unified Batch/Streaming**: Single API for both workloads

# COMMAND ----------

# Create a sample Delta table to demonstrate functionality
print("=== Delta Lake Demonstration ===")

# Create sample data
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# Sample data representing user activities
sample_data = [
    Row(user_id=1, activity="login", timestamp=datetime.now(), session_id="sess_001"),
    Row(user_id=2, activity="view_product", timestamp=datetime.now(), session_id="sess_002"),
    Row(user_id=1, activity="add_to_cart", timestamp=datetime.now(), session_id="sess_001"),
    Row(user_id=3, activity="login", timestamp=datetime.now(), session_id="sess_003"),
    Row(user_id=2, activity="purchase", timestamp=datetime.now(), session_id="sess_002")
]

# Create DataFrame
df = spark.createDataFrame(sample_data)

print("Sample user activity data:")
df.show(truncate=False)

print("\nDataFrame Schema:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Lakehouse Architecture
# MAGIC 
# MAGIC The Data Lakehouse combines the best of data warehouses and data lakes:
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Analytics & ML Layer                     │
# MAGIC │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
# MAGIC │  │ BI Tools    │  │ ML Platforms│  │ Analytics Apps      │  │
# MAGIC │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    Databricks Lakehouse                     │
# MAGIC │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
# MAGIC │  │ Unity       │  │ Delta Lake  │  │ Apache Spark        │  │
# MAGIC │  │ Catalog     │  │ (ACID,      │  │ (Compute Engine)    │  │
# MAGIC │  │ (Governance)│  │ Time Travel)│  │                     │  │
# MAGIC │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC                              │
# MAGIC                              ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                      Storage Layer                          │
# MAGIC │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
# MAGIC │  │ Cloud       │  │ On-Premises │  │ Multi-Cloud         │  │
# MAGIC │  │ Storage     │  │ Storage     │  │ Storage             │  │
# MAGIC │  │ (S3, ADLS,  │  │ (HDFS, NFS) │  │ (Hybrid)            │  │
# MAGIC │  │ GCS)        │  │             │  │                     │  │
# MAGIC │  └─────────────┘  └─────────────┘  └─────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ### Lakehouse Benefits
# MAGIC 
# MAGIC 1. **Single Source of Truth**: All data in one place
# MAGIC 2. **ACID Transactions**: Reliable data operations
# MAGIC 3. **Schema Enforcement**: Data quality guarantees
# MAGIC 4. **Performance**: Optimized for analytics and ML
# MAGIC 5. **Cost-Effective**: Efficient storage and compute

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Optimization Techniques
# MAGIC 
# MAGIC Databricks provides several optimization techniques:

# COMMAND ----------

# Demonstrate basic performance optimization concepts
print("=== Performance Optimization Examples ===")

# 1. Partitioning data for better query performance
print("\n1. Data Partitioning:")
print("   - Partition by frequently queried columns (e.g., date, region)")
print("   - Example: df.write.partitionBy('date').save('/path/to/data')")

# 2. Using appropriate file formats
print("\n2. File Format Optimization:")
print("   - Parquet: Column-oriented, compressed, schema evolution")
print("   - Delta: Parquet + ACID transactions + time travel")
print("   - Avoid CSV for large datasets in production")

# 3. Cluster sizing
print("\n3. Cluster Optimization:")
print("   - Right-size clusters based on workload")
print("   - Use auto-scaling for variable workloads")
print("   - Consider spot instances for cost optimization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC 
# MAGIC ### Development Best Practices
# MAGIC 
# MAGIC 1. **Use Delta Lake**: For all production data storage
# MAGIC 2. **Implement Data Quality**: Validate data at ingestion
# MAGIC 3. **Optimize Queries**: Use proper partitioning and indexing
# MAGIC 4. **Monitor Performance**: Use Spark UI and Databricks metrics
# MAGIC 5. **Version Control**: Use Git integration for notebooks
# MAGIC 
# MAGIC ### Production Best Practices
# MAGIC 
# MAGIC 1. **Use Jobs**: Schedule and monitor production workloads
# MAGIC 2. **Implement CI/CD**: Automate testing and deployment
# MAGIC 3. **Monitor Costs**: Use cluster policies and auto-termination
# MAGIC 4. **Secure Data**: Implement proper access controls
# MAGIC 5. **Backup Strategies**: Use Delta Lake time travel and external backups
# MAGIC 
# MAGIC ### Collaboration Best Practices
# MAGIC 
# MAGIC 1. **Shared Workspaces**: Organize teams and projects
# MAGIC 2. **Documentation**: Document code and processes
# MAGIC 3. **Code Reviews**: Use collaborative development practices
# MAGIC 4. **Standards**: Establish coding and naming conventions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Continue to the next notebooks in this series:
# MAGIC 
# MAGIC 1. **02_unity_catalog_deep_dive**: Learn about data governance and catalog structure
# MAGIC 2. **03_cluster_management**: Master compute resources and optimization
# MAGIC 3. **04_spark_on_databricks**: Deep dive into distributed computing
# MAGIC 4. **05_delta_lake_concepts_explained**: Explore Delta Lake's ACID transactions and schema management
# MAGIC 
# MAGIC Each notebook builds upon the concepts introduced here and provides hands-on experience with Databricks features.

# COMMAND ----------

# Clean up resources
print("Resources cleaned up. Ready for the next notebook!")