# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Management and Optimization - Week 1
# MAGIC 
# MAGIC This notebook provides comprehensive coverage of Databricks cluster management, including cluster types, optimization strategies, cost management, and performance tuning.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC - Understand different cluster types and their use cases
# MAGIC - Master cluster configuration and sizing strategies
# MAGIC - Learn auto-scaling and cost optimization techniques
# MAGIC - Explore performance monitoring and troubleshooting
# MAGIC - Implement cluster policies and governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Types Overview
# MAGIC 
# MAGIC Databricks offers several cluster types optimized for different workloads:
# MAGIC 
# MAGIC ### 1. All-Purpose Clusters (Interactive)
# MAGIC ```
# MAGIC Use Cases:
# MAGIC ├── Data Exploration
# MAGIC ├── Notebook Development
# MAGIC ├── Ad-hoc Analysis
# MAGIC ├── Model Training (interactive)
# MAGIC └── Prototyping
# MAGIC 
# MAGIC Characteristics:
# MAGIC ├── Always-on (until terminated)
# MAGIC ├── Shared among multiple users
# MAGIC ├── Best for development/exploration
# MAGIC └── Higher cost per DBU
# MAGIC ```
# MAGIC 
# MAGIC ### 2. Job Clusters (Automated)
# MAGIC ```
# MAGIC Use Cases:
# MAGIC ├── Scheduled ETL Jobs
# MAGIC ├── Production Workflows
# MAGIC ├── Batch Processing
# MAGIC ├── Model Training (automated)
# MAGIC └── Data Pipelines
# MAGIC 
# MAGIC Characteristics:
# MAGIC ├── Created for specific job
# MAGIC ├── Automatically terminated
# MAGIC ├── Cost-effective for production
# MAGIC └── Isolated execution environment
# MAGIC ```
# MAGIC 
# MAGIC ### 3. SQL Warehouses (Analytics)
# MAGIC ```
# MAGIC Use Cases:
# MAGIC ├── BI and Reporting
# MAGIC ├── SQL Analytics
# MAGIC ├── Dashboard Queries
# MAGIC ├── Ad-hoc SQL Queries
# MAGIC └── Data Visualization
# MAGIC
# MAGIC Characteristics:
# MAGIC ├── Optimized for SQL workloads
# MAGIC ├── Serverless compute option
# MAGIC ├── Auto-suspend capability
# MAGIC └── Multi-user concurrent access
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep Dive: Databricks Compute Options
# MAGIC
# MAGIC ### 1. All-Purpose Clusters (Interactive Development)
# MAGIC
# MAGIC **What They Are:**
# MAGIC - Traditional Apache Spark clusters that you manually create and manage
# MAGIC - Persistent compute resources that stay running until explicitly terminated
# MAGIC - Ideal for interactive data exploration and notebook development
# MAGIC
# MAGIC **Key Features:**
# MAGIC - **Multi-user Support**: Multiple users can attach notebooks to the same cluster
# MAGIC - **Manual Lifecycle Management**: You control when clusters start and stop
# MAGIC - **Full Customization**: Configure driver/worker types, auto-scaling, Spark configs
# MAGIC - **Always Available**: Stays running for quick notebook execution
# MAGIC - **Cost Model**: Charged per DBU while cluster is running (even if idle)
# MAGIC
# MAGIC **Best For:**
# MAGIC - Data exploration and ad-hoc analysis
# MAGIC - Notebook development and prototyping
# MAGIC - Model training experiments
# MAGIC - Collaborative team development
# MAGIC
# MAGIC **Configuration Example:**
# MAGIC ```python
# MAGIC {
# MAGIC   "cluster_name": "data-exploration-cluster",
# MAGIC   "spark_version": "13.3.x-scala2.12",
# MAGIC   "node_type_id": "i3.xlarge",
# MAGIC   "driver_node_type_id": "i3.xlarge",
# MAGIC   "autoscale": {
# MAGIC     "min_workers": 1,
# MAGIC     "max_workers": 8
# MAGIC   },
# MAGIC   "auto_termination_minutes": 120
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Job Clusters (Production Workloads)
# MAGIC
# MAGIC **What They Are:**
# MAGIC - Ephemeral clusters created automatically when a Databricks Job runs
# MAGIC - Automatically terminated when the job completes
# MAGIC - Optimized for scheduled production workloads
# MAGIC
# MAGIC **Key Features:**
# MAGIC - **Automatic Lifecycle**: Created at job start, terminated at job end
# MAGIC - **Isolated Execution**: Each job runs on its own dedicated cluster
# MAGIC - **Lower Cost**: ~30-40% cheaper than all-purpose clusters per DBU
# MAGIC - **Reproducibility**: Same cluster config every time
# MAGIC - **No Idle Time**: Only pay for actual job execution
# MAGIC
# MAGIC **Best For:**
# MAGIC - Scheduled ETL pipelines
# MAGIC - Production data processing workflows
# MAGIC - Automated ML model training
# MAGIC - Batch data transformations
# MAGIC
# MAGIC **Job Cluster vs All-Purpose Cost Comparison:**
# MAGIC ```
# MAGIC All-Purpose Cluster: $0.40/DBU  (24/7 availability)
# MAGIC Job Cluster:        $0.15/DBU  (only during execution)
# MAGIC
# MAGIC For a 4-hour daily ETL job:
# MAGIC All-Purpose: 24 hours × $0.40 = $9.60/day
# MAGIC Job Cluster:  4 hours × $0.15 = $0.60/day
# MAGIC Savings: ~94% cost reduction
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. SQL Warehouses (Analytics & BI)
# MAGIC
# MAGIC **What They Are:**
# MAGIC - Specialized compute engines optimized for SQL queries
# MAGIC - Support both classic (provisioned) and serverless modes
# MAGIC - Built for concurrent user access and fast query performance
# MAGIC
# MAGIC **Key Features:**
# MAGIC - **SQL-Optimized**: Photon engine for accelerated query performance
# MAGIC - **Auto-Suspend/Resume**: Automatically stops after inactivity
# MAGIC - **Concurrent Queries**: Handle multiple users simultaneously
# MAGIC - **Query Caching**: Intelligent result caching for repeated queries
# MAGIC - **BI Tool Integration**: Native connectors for Tableau, Power BI, Looker
# MAGIC
# MAGIC **Warehouse Types:**
# MAGIC
# MAGIC **Classic SQL Warehouse:**
# MAGIC - Traditional cluster-based compute
# MAGIC - You manage size (X-Small to 4X-Large)
# MAGIC - Predictable performance and cost
# MAGIC
# MAGIC **Serverless SQL Warehouse:**
# MAGIC - Instant startup (no cluster provisioning wait)
# MAGIC - Auto-scaling compute managed by Databricks
# MAGIC - Pay only for query execution time
# MAGIC - Ideal for variable/unpredictable workloads
# MAGIC
# MAGIC **Best For:**
# MAGIC - BI dashboards and reporting
# MAGIC - Ad-hoc SQL analytics
# MAGIC - Data analyst self-service queries
# MAGIC - Real-time data exploration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Serverless Compute (Databricks Free Edition)
# MAGIC
# MAGIC **What It Is:**
# MAGIC - Fully managed compute where Databricks handles all infrastructure
# MAGIC - Available for both notebooks and SQL warehouses
# MAGIC - The ONLY compute option available in Databricks Free Edition
# MAGIC
# MAGIC **Key Characteristics:**
# MAGIC - **Zero Configuration**: No cluster sizing or instance type selection
# MAGIC - **Instant Start**: No cluster startup wait time (notebooks start immediately)
# MAGIC - **Automatic Scaling**: Databricks manages all resource allocation
# MAGIC - **Simplified Experience**: No cluster management UI or options
# MAGIC - **Usage-Based Billing**: Pay only for actual compute time (not available in Free Edition)
# MAGIC
# MAGIC **Free Edition Limitations:**
# MAGIC - Cannot create all-purpose clusters (serverless notebooks only)
# MAGIC - Cannot create job clusters (serverless jobs only)
# MAGIC - Limited to serverless SQL warehouses
# MAGIC - No cluster configuration or customization options
# MAGIC
# MAGIC **Important for This Course:**
# MAGIC ```
# MAGIC ✅ Free Edition Users:
# MAGIC   - All notebooks in this course work on serverless compute
# MAGIC   - No cluster configuration required
# MAGIC   - Just open notebook and run
# MAGIC
# MAGIC ✅ Classic Trial Users:
# MAGIC   - Can create all-purpose clusters for deeper learning
# MAGIC   - Explore cluster sizing and auto-scaling concepts
# MAGIC   - Hands-on experience with cluster configuration
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Photon Engine (Query Acceleration)
# MAGIC
# MAGIC **What It Is:**
# MAGIC - Databricks' native vectorized query engine written in C++
# MAGIC - Replaces parts of Apache Spark engine for significant performance gains
# MAGIC - Available on SQL Warehouses and certain cluster types
# MAGIC
# MAGIC **Performance Benefits:**
# MAGIC - **3-10x faster** queries compared to standard Spark
# MAGIC - **Vectorized Execution**: Processes data in columnar batches
# MAGIC - **Reduced Costs**: Faster execution = less compute time
# MAGIC - **Automatic Optimization**: No code changes required
# MAGIC
# MAGIC **How It Works:**
# MAGIC ```
# MAGIC Standard Spark:
# MAGIC Row 1 → Process → Result 1
# MAGIC Row 2 → Process → Result 2
# MAGIC Row 3 → Process → Result 3
# MAGIC (One row at a time)
# MAGIC
# MAGIC Photon Engine:
# MAGIC [Row 1, Row 2, Row 3, ..., Row 1024]
# MAGIC → Process batch in parallel →
# MAGIC [Result 1, Result 2, Result 3, ..., Result 1024]
# MAGIC (Vectorized batch processing)
# MAGIC ```
# MAGIC
# MAGIC **Best For:**
# MAGIC - SQL-heavy workloads (SELECT, JOIN, GROUP BY)
# MAGIC - Large-scale aggregations
# MAGIC - Delta Lake table scans
# MAGIC - Analytics and reporting queries
# MAGIC
# MAGIC **Where Available:**
# MAGIC - ✅ SQL Warehouses (always enabled)
# MAGIC - ✅ Serverless compute (automatically enabled)
# MAGIC - ✅ All-purpose clusters (enable via runtime selection: "Photon" runtime)
# MAGIC - ✅ Job clusters (enable via configuration)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. Delta Live Tables (DLT) Compute
# MAGIC
# MAGIC **What It Is:**
# MAGIC - Declarative framework for building reliable data pipelines
# MAGIC - Automatically manages underlying compute infrastructure
# MAGIC - Uses dedicated DLT clusters optimized for streaming and batch ETL
# MAGIC
# MAGIC **Cluster Architecture:**
# MAGIC ```
# MAGIC DLT Pipeline:
# MAGIC ├── Dedicated DLT Cluster (auto-managed)
# MAGIC │   ├── Driver node (pipeline orchestration)
# MAGIC │   └── Worker nodes (data processing)
# MAGIC ├── Photon enabled by default
# MAGIC ├── Auto-scaling based on workload
# MAGIC └── Auto-recovery on failures
# MAGIC ```
# MAGIC
# MAGIC **Key Features:**
# MAGIC - **Automatic Cluster Management**: DLT creates and manages clusters for you
# MAGIC - **Enhanced Autoscaling**: Scales based on streaming data velocity
# MAGIC - **Pipeline Isolation**: Each DLT pipeline gets its own compute
# MAGIC - **Built-in Monitoring**: Data quality and pipeline health metrics
# MAGIC - **Automatic Retries**: Fault tolerance and error recovery
# MAGIC
# MAGIC **Compute Modes:**
# MAGIC
# MAGIC **1. Triggered (Batch) Mode:**
# MAGIC - Cluster starts when pipeline is triggered
# MAGIC - Processes all data and terminates
# MAGIC - Like job clusters but DLT-optimized
# MAGIC
# MAGIC **2. Continuous (Streaming) Mode:**
# MAGIC - Cluster stays running continuously
# MAGIC - Processes streaming data in real-time
# MAGIC - Auto-scales based on incoming data rate
# MAGIC
# MAGIC **DLT Cluster vs Traditional Cluster:**
# MAGIC ```
# MAGIC Traditional Cluster:
# MAGIC - You write complex retry logic
# MAGIC - You manage checkpoints
# MAGIC - You monitor data quality manually
# MAGIC - You configure auto-scaling
# MAGIC
# MAGIC DLT Cluster:
# MAGIC - Automatic retry and recovery
# MAGIC - Automatic checkpoint management
# MAGIC - Built-in data quality checks
# MAGIC - Intelligent auto-scaling for streaming
# MAGIC ```
# MAGIC
# MAGIC **Best For:**
# MAGIC - Medallion architecture (Bronze → Silver → Gold)
# MAGIC - Streaming data pipelines
# MAGIC - CDC (Change Data Capture) workflows
# MAGIC - Complex ETL with data quality requirements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Compute Options Comparison Table
# MAGIC
# MAGIC | Feature | All-Purpose | Job Cluster | SQL Warehouse | Serverless | DLT |
# MAGIC |---------|-------------|-------------|---------------|------------|-----|
# MAGIC | **Use Case** | Development | Production Jobs | Analytics | Free Edition | ETL Pipelines |
# MAGIC | **Startup Time** | 5-7 minutes | 5-7 minutes | 1-2 minutes | Instant | Auto-managed |
# MAGIC | **Cost** | $$$ | $$ | $$ | Pay-per-use | $$ |
# MAGIC | **Auto-Terminate** | Manual/Timer | Automatic | Auto-suspend | N/A | Pipeline-based |
# MAGIC | **Photon** | Optional | Optional | Always | Always | Always |
# MAGIC | **Multi-User** | ✅ Yes | ❌ No | ✅ Yes | ✅ Yes | ❌ No |
# MAGIC | **Customization** | Full | Full | Limited | None | Limited |
# MAGIC | **Free Edition** | ❌ No | ❌ No | ✅ Yes (serverless) | ✅ Yes | ❌ No |
# MAGIC | **Best For** | Exploration | Automation | SQL/BI | Learning | Data Engineering |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Choosing the Right Compute
# MAGIC
# MAGIC **Development Phase:**
# MAGIC - Free Edition → Serverless notebooks (zero config)
# MAGIC - Classic Trial → All-purpose clusters (learn configuration)
# MAGIC
# MAGIC **Production Phase:**
# MAGIC - Scheduled Workloads → Job clusters (cost-effective)
# MAGIC - Analytics/Dashboards → SQL Warehouses with Photon
# MAGIC - ETL Pipelines → Delta Live Tables (reliability + automation)
# MAGIC - Real-time Streaming → DLT continuous mode
# MAGIC
# MAGIC **Cost Optimization:**
# MAGIC - Replace all-purpose with job clusters for scheduled work (-60% cost)
# MAGIC - Use serverless SQL for variable workloads (pay only for queries)
# MAGIC - Enable Photon for faster queries (less compute time = lower cost)
# MAGIC - Use DLT for complex pipelines (reduce development + maintenance time)

# COMMAND ----------

# Get current cluster information
print("=== Current Cluster Information ===")

import os
import json

# Cluster metadata
cluster_id = os.getenv('DATABRICKS_CLUSTER_ID', 'Not available in serverless compute')
print(f"Cluster ID: {cluster_id}")

# Spark configuration
print(f"\nSpark Version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Sizing Strategy
# MAGIC 
# MAGIC ### Memory Allocation Breakdown
# MAGIC 
# MAGIC ```
# MAGIC Total Node Memory (100%)
# MAGIC ├── System Reserved (10-15%)
# MAGIC ├── Spark Framework (10-15%) 
# MAGIC └── Available for Spark (70-80%)
# MAGIC     ├── Storage Memory (60% default)
# MAGIC     │   ├── Cached DataFrames
# MAGIC     │   ├── Broadcast Variables
# MAGIC     │   └── Spark Internal Data
# MAGIC     └── Execution Memory (40% default)
# MAGIC         ├── Shuffles
# MAGIC         ├── Joins
# MAGIC         ├── Sorts
# MAGIC         └── Aggregations
# MAGIC ```
# MAGIC 
# MAGIC ### Sizing Guidelines
# MAGIC 
# MAGIC | Workload Type | Driver Size | Worker Size | Worker Count |
# MAGIC |---------------|-------------|-------------|--------------|
# MAGIC | **ETL (Small)** | Standard | Standard | 2-4 |
# MAGIC | **ETL (Large)** | Large | Large | 8-16 |
# MAGIC | **ML Training** | Large | Memory Optimized | 4-8 |
# MAGIC | **Streaming** | Standard | Standard | Auto-scale |
# MAGIC | **Analytics** | Standard | Standard | 2-8 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example of Cluster Analysis
# MAGIC ### Reference only. 
# MAGIC This example cannot be performed on Databricks Free Edition, but can be studied with open-source Apache Spark cluster locally or in cloud.
# MAGIC
# MAGIC ```
# MAGIC # Analyze current cluster resource utilization
# MAGIC print("=== Resource Utilization Analysis ===")
# MAGIC
# MAGIC # Examine current configuration
# MAGIC important_configs = [
# MAGIC    'spark.sql.adaptive.enabled',
# MAGIC    'spark.sql.adaptive.coalescePartitions.enabled',
# MAGIC    'spark.sql.adaptive.skewJoin.enabled',
# MAGIC    'spark.serializer',
# MAGIC    'spark.sql.execution.arrow.pyspark.enabled'
# MAGIC ]
# MAGIC
# MAGIC print("\nImportant Spark Configurations:")
# MAGIC for config in important_configs:
# MAGIC    try:
# MAGIC        value = spark.conf.get(config)
# MAGIC    except Exception:
# MAGIC        value = "Not available"
# MAGIC    print(f"  {config}: {value}")
# MAGIC ```

# COMMAND ----------

# Demonstrate optimal data loading and partitioning
print("=== Data Partitioning and Performance ===")

from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

# Create a larger dataset to demonstrate partitioning concepts
print("Creating sample dataset for performance testing...")

# Generate sample e-commerce data
from datetime import datetime, timedelta
import random

# Create a time series of orders
base_date = datetime(2024, 1, 1)
sample_data = []

for i in range(10000):  # 10K records for demonstration
    order_date = base_date + timedelta(days=random.randint(0, 365))
    sample_data.append({
        'order_id': i + 1,
        'customer_id': random.randint(1, 1000),
        'product_category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports']),
        'order_amount': round(random.uniform(10.0, 500.0), 2),
        'order_date': order_date.date(),
        'region': random.choice(['North', 'South', 'East', 'West'])
    })

# Create DataFrame
df = spark.createDataFrame(sample_data)
print(f"Created dataset with {df.count()} records")

# Check default partitioning
print(f"Default number of partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# Demonstrate partitioning strategies
print("=== Partitioning Strategies Comparison ===")

# 1. Default partitioning performance
start_time = time.time()
result1 = df.groupBy("product_category").agg(F.sum("order_amount").alias("total_sales")).collect()
default_time = time.time() - start_time

print(f"1. Default partitioning time: {default_time:.3f} seconds")

# 2. Repartition by key for better performance
start_time = time.time()
df_repartitioned = df.repartition(4, "product_category")
result2 = df_repartitioned.groupBy("product_category").agg(F.sum("order_amount").alias("total_sales")).collect()
repartition_time = time.time() - start_time

print(f"2. Repartitioned by key time: {repartition_time:.3f} seconds")
print(f"   Partitions after repartition: {spark.conf.get('spark.sql.shuffle.partitions')}")

# 3. Coalesce to reduce partitions
start_time = time.time()
df_coalesced = df.coalesce(2)
result3 = df_coalesced.groupBy("product_category").agg(F.sum("order_amount").alias("total_sales")).collect()
coalesce_time = time.time() - start_time

print(f"3. Coalesced partitions time: {coalesce_time:.3f} seconds")
print(f"   Partitions after coalesce: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Display results
print("\nAggregation Results:")
for row in result1:
    print(f"  {row['product_category']}: ${row['total_sales']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-Scaling Configuration
# MAGIC 
# MAGIC ### Auto-scaling Benefits
# MAGIC 
# MAGIC 1. **Cost Optimization**: Scale down during low usage
# MAGIC 2. **Performance**: Scale up for demanding workloads  
# MAGIC 3. **Resource Efficiency**: Match compute to workload
# MAGIC 4. **Simplified Management**: Automatic cluster sizing
# MAGIC 
# MAGIC ### Auto-scaling Strategies
# MAGIC 
# MAGIC ```json
# MAGIC {
# MAGIC   "cluster_config": {
# MAGIC     "autoscale": {
# MAGIC       "min_workers": 1,
# MAGIC       "max_workers": 8
# MAGIC     },
# MAGIC     "auto_termination_minutes": 120,
# MAGIC     "enable_elastic_disk": true
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ### Scaling Triggers
# MAGIC 
# MAGIC - **Scale Up**: High CPU/memory utilization, pending tasks
# MAGIC - **Scale Down**: Low utilization, idle workers
# MAGIC - **Termination**: Inactivity timeout reached

# COMMAND ----------

# Simulate workload scaling scenarios
print("=== Auto-scaling Simulation ===")

# Demonstrate different workload patterns
workload_scenarios = [
    ("Light ETL", 1000),
    ("Medium Analytics", 5000), 
    ("Heavy ML Training", 20000),
    ("Batch Processing", 50000)
]

for scenario_name, record_count in workload_scenarios:
    print(f"\n--- {scenario_name} Scenario ({record_count:,} records) ---")
    
    # Create appropriately sized dataset
    scenario_data = sample_data[:record_count]
    scenario_df = spark.createDataFrame(scenario_data)
    
    # Recommend partitioning strategy
    recommended_partitions = max(2, min(record_count // 1000, 16))
    print(f"Recommended partitions: {recommended_partitions}")
    
    # Calculate optimal worker count (rough estimation)
    estimated_workers = max(1, min(recommended_partitions // 2, 8))
    print(f"Estimated optimal workers: {estimated_workers}")
    
    # Perform typical operations for this workload
    start_time = time.time()
    
    if "ETL" in scenario_name:
        # ETL-style operations
        result = scenario_df.filter(F.col("order_amount") > 100) \
                           .groupBy("region") \
                           .agg(F.avg("order_amount").alias("avg_amount")) \
                           .collect()
    elif "Analytics" in scenario_name:
        # Analytics-style operations  
        result = scenario_df.groupBy("product_category", "region") \
                           .agg(F.sum("order_amount").alias("revenue"),
                                F.count("order_id").alias("order_count")) \
                           .collect()
    else:
        # ML/Batch-style operations
        result = scenario_df.select("*", 
                                   (F.col("order_amount") * 0.1).alias("tax"),
                                   F.when(F.col("order_amount") > 200, "premium")
                                    .otherwise("standard").alias("tier")) \
                           .collect()
    
    execution_time = time.time() - start_time
    print(f"Execution time: {execution_time:.3f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Monitoring and Optimization
# MAGIC 
# MAGIC ### Key Performance Metrics
# MAGIC 
# MAGIC 1. **Cluster Metrics**
# MAGIC    - CPU utilization
# MAGIC    - Memory usage
# MAGIC    - Network I/O
# MAGIC    - Disk I/O
# MAGIC 
# MAGIC 2. **Spark Metrics**
# MAGIC    - Task execution time
# MAGIC    - Shuffle read/write
# MAGIC    - GC time
# MAGIC    - Stage duration
# MAGIC 
# MAGIC 3. **Application Metrics**
# MAGIC    - Query execution time
# MAGIC    - Data processing throughput
# MAGIC    - Error rates
# MAGIC    - Resource utilization efficiency

# COMMAND ----------

# Performance monitoring demonstration
print("=== Performance Monitoring ===")

# Create a more complex workload for monitoring
complex_df = df.select("*") \
    .withColumn("month", F.month("order_date")) \
    .withColumn("year", F.year("order_date")) \
    .withColumn("quarter", F.quarter("order_date"))

# Monitor multiple operations
operations = [
    ("Aggregation by month", lambda df: df.groupBy("year", "month").agg(F.sum("order_amount")).collect()),
    ("Join operation", lambda df: df.alias("a").join(df.select("customer_id").distinct().alias("b"), "customer_id").collect()),
    ("Window function", lambda df: df.withColumn("running_total", 
                                                 F.sum("order_amount").over(
                                                     Window.partitionBy("customer_id").orderBy("order_date")
                                                 )).collect())
]

from pyspark.sql import Window

for op_name, operation in operations:
    start_time = time.time()
    
    try:
        result = operation(complex_df)
        execution_time = time.time() - start_time
        print(f"{op_name}: {execution_time:.3f} seconds ({len(result)} results)")
    except Exception as e:
        print(f"{op_name}: Error - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Optimization Strategies
# MAGIC 
# MAGIC ### Cost Factors
# MAGIC 
# MAGIC 1. **Compute Costs**
# MAGIC    - Instance types and sizes
# MAGIC    - Running time
# MAGIC    - Idle time
# MAGIC 
# MAGIC 2. **Storage Costs**
# MAGIC    - Data storage volume
# MAGIC    - Storage class (hot/cold)
# MAGIC    - Retention policies
# MAGIC 
# MAGIC 3. **Network Costs**
# MAGIC    - Data transfer
# MAGIC    - Cross-region traffic
# MAGIC    - External integrations
# MAGIC 
# MAGIC ### Optimization Techniques
# MAGIC 
# MAGIC ```python
# MAGIC # 1. Right-size clusters
# MAGIC cluster_config = {
# MAGIC     "driver_node_type": "i3.large",    # Match workload needs
# MAGIC     "node_type": "i3.large",           # Balance cost/performance
# MAGIC     "min_workers": 1,                  # Start small
# MAGIC     "max_workers": 4,                  # Limit maximum scale
# MAGIC     "auto_termination_minutes": 60     # Aggressive termination
# MAGIC }
# MAGIC 
# MAGIC # 2. Use spot instances for non-critical workloads
# MAGIC spot_config = {
# MAGIC     "aws_attributes": {
# MAGIC         "first_on_demand": 1,          # Reliable driver
# MAGIC         "availability": "SPOT_WITH_FALLBACK"
# MAGIC     }
# MAGIC }
# MAGIC 
# MAGIC # 3. Optimize data storage
# MAGIC storage_optimization = {
# MAGIC     "enable_delta_optimization": True,
# MAGIC     "partition_strategy": "by_date",
# MAGIC     "compression": "snappy",
# MAGIC     "retention_days": 30
# MAGIC }
# MAGIC ```

# COMMAND ----------

# Cost analysis demonstration
print("=== Cost Optimization Analysis ===")

# Simulate cost comparison scenarios
scenarios = {
    "Small Development": {
        "driver": "i3.large",
        "workers": "i3.large", 
        "worker_count": 2,
        "hours_per_day": 8,
        "days_per_month": 20
    },
    "Medium Production": {
        "driver": "i3.xlarge",
        "workers": "i3.xlarge",
        "worker_count": 4, 
        "hours_per_day": 24,
        "days_per_month": 30
    },
    "Large Analytics": {
        "driver": "r5.2xlarge",
        "workers": "r5.xlarge",
        "worker_count": 8,
        "hours_per_day": 12,
        "days_per_month": 30
    }
}

# Simplified cost calculation (actual rates vary by region and contract)
instance_costs = {
    "i3.large": 0.156,
    "i3.xlarge": 0.312,
    "r5.xlarge": 0.252,
    "r5.2xlarge": 0.504
}

print("Monthly Cost Estimates (USD):")
print("-" * 50)

for scenario_name, config in scenarios.items():
    driver_cost = instance_costs[config["driver"]] * config["hours_per_day"] * config["days_per_month"]
    worker_cost = instance_costs[config["workers"]] * config["worker_count"] * config["hours_per_day"] * config["days_per_month"]
    total_cost = driver_cost + worker_cost
    
    print(f"{scenario_name:20} | ${total_cost:8.2f}")
    print(f"  Driver ({config['driver']:12}): ${driver_cost:8.2f}")
    print(f"  Workers ({config['worker_count']}x {config['workers']:10}): ${worker_cost:8.2f}")
    print()

print("💡 Cost Optimization Tips:")
print("- Use auto-termination to avoid idle costs")
print("- Consider spot instances for fault-tolerant workloads") 
print("- Right-size based on actual usage patterns")
print("- Use job clusters for production workflows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cluster Policies and Governance
# MAGIC 
# MAGIC ### Policy Categories
# MAGIC 
# MAGIC 1. **Resource Limits**
# MAGIC    ```json
# MAGIC    {
# MAGIC      "max_workers": 10,
# MAGIC      "max_driver_memory": "16g",
# MAGIC      "allowed_instance_types": ["i3.large", "i3.xlarge"]
# MAGIC    }
# MAGIC    ```
# MAGIC 
# MAGIC 2. **Security Requirements**
# MAGIC    ```json
# MAGIC    {
# MAGIC      "enable_credential_passthrough": false,
# MAGIC      "required_tags": ["environment", "cost_center"],
# MAGIC      "cluster_log_delivery": "required"
# MAGIC    }
# MAGIC    ```
# MAGIC 
# MAGIC 3. **Cost Controls**
# MAGIC    ```json
# MAGIC    {
# MAGIC      "auto_termination_minutes": 120,
# MAGIC      "enable_elastic_disk": true,
# MAGIC      "preemptible_instances": true
# MAGIC    }
# MAGIC    ```

# COMMAND ----------

# Demonstrate policy compliance checking
print("=== Cluster Policy Compliance ===")

# Define sample policy rules
policy_rules = {
    "max_workers": 8,
    "max_auto_termination_minutes": 180,
    "required_spark_configs": [
        "spark.sql.adaptive.enabled",
        "spark.serializer"
    ],
    "forbidden_configs": [
        "spark.sql.execution.arrow.maxRecordsPerBatch"
    ]
}

# Check current cluster against policy
print("Policy Compliance Check:")

# Check worker count (simulated)
current_workers = 2  # This would come from cluster metadata
compliance_status = "✓" if current_workers <= policy_rules["max_workers"] else "✗"
print(f"{compliance_status} Worker count: {current_workers}/{policy_rules['max_workers']}")

# Check Spark configurations
print(f"\nSpark Configuration Compliance:")
for required_config in policy_rules["required_spark_configs"]:
    try:
        value = spark.conf.get(required_config, None)
        status = "✓" if value is not None else "✗"
        print(f"{status} {required_config}: {value}")
    except Exception:
        value = "Not available"
        print(f"{required_config}: {value}")

# Policy recommendations
print(f"\n📋 Policy Recommendations:")
print(f"- Set auto-termination ≤ {policy_rules['max_auto_termination_minutes']} minutes")
print(f"- Enable adaptive query execution")
print(f"- Use Kryo serializer for better performance")
print(f"- Implement cluster tagging for cost tracking")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common Issues
# MAGIC 
# MAGIC ### Performance Issues
# MAGIC 
# MAGIC | Symptom | Likely Cause | Solution |
# MAGIC |---------|--------------|----------|
# MAGIC | **Slow queries** | Poor partitioning | Repartition by join/group keys |
# MAGIC | **OOM errors** | Skewed data | Use salting or custom partitioning |
# MAGIC | **High GC time** | Too much cached data | Reduce cache usage, tune memory |
# MAGIC | **Shuffle spill** | Insufficient memory | Increase executor memory |
# MAGIC 
# MAGIC ### Resource Issues
# MAGIC 
# MAGIC | Symptom | Likely Cause | Solution |
# MAGIC |---------|--------------|----------|
# MAGIC | **Cluster startup delays** | Cold starts | Use cluster pools |
# MAGIC | **Auto-scaling lag** | Conservative scaling | Tune scaling parameters |
# MAGIC | **Resource contention** | Oversubscription | Increase cluster size |
# MAGIC | **Network bottlenecks** | Data locality | Optimize data placement |

# COMMAND ----------

# Troubleshooting toolkit
print("=== Cluster Troubleshooting Toolkit ===")

# 1. Memory analysis
def analyze_memory_usage():
    """Analyze current memory usage patterns"""
    try:
        total_memory = spark.conf.get('spark.executor.memory')
        storage_fraction = spark.sparkContext.getConf().get('spark.sql.execution.arrow.maxRecordsPerBatch', '0.6')

        print(f"Executor memory: {total_memory}")
        print(f"Storage Memory Fraction: {storage_fraction}")
    except Exception as e:
        print(f"Executor memory: Not accessible on serverless compute")
    
    # Check for cached datasets
    cached_tables = spark.sql("SHOW TABLES").filter("isTemporary = true").collect()
    print(f"Cached temporary tables: {len(cached_tables)}")

# 2. Performance diagnostics
def diagnose_performance(df):
    """Basic performance diagnostics for a DataFrame"""
    print(f"DataFrame partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print(f"Estimated DataFrame size: {df.count()} rows")
    
    # Check for skewed partitions
    from pyspark.sql.functions import spark_partition_id, count
    partition_sizes = (df
        .groupBy(spark_partition_id().alias("partition_id"))
        .agg(count("*").alias("row_count"))
        .orderBy("partition_id")
        .collect()
    )
    partition_sizes = [(row.partition_id, row.row_count) for row in partition_sizes]
    sizes = [size for _, size in partition_sizes]
    
    if sizes:
        avg_size = sum(sizes) / len(sizes)
        max_size = max(sizes)
        skew_ratio = max_size / avg_size if avg_size > 0 else 0
        
        print(f"Partition size distribution:")
        print(f"  Average: {avg_size:.0f} rows")
        print(f"  Maximum: {max_size:.0f} rows") 
        print(f"  Skew ratio: {skew_ratio:.2f}")
        
        if skew_ratio > 2.0:
            print("⚠️  High partition skew detected - consider repartitioning")

# Run diagnostics
print("Running memory analysis...")
analyze_memory_usage()

print("\nRunning performance diagnostics on sample data...")
diagnose_performance(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC 
# MAGIC ### Cluster Configuration
# MAGIC 
# MAGIC 1. **Choose the Right Type**
# MAGIC    - Interactive clusters for development
# MAGIC    - Job clusters for production workloads
# MAGIC    - SQL warehouses for analytics
# MAGIC 
# MAGIC 2. **Size Appropriately**
# MAGIC    - Start small, scale based on needs
# MAGIC    - Use auto-scaling for variable workloads
# MAGIC    - Monitor utilization regularly
# MAGIC 
# MAGIC 3. **Optimize for Cost**
# MAGIC    - Set aggressive auto-termination
# MAGIC    - Use spot instances when appropriate
# MAGIC    - Implement cluster pools for frequent starts
# MAGIC 
# MAGIC ### Performance Optimization
# MAGIC 
# MAGIC 1. **Data Partitioning**
# MAGIC    - Partition by frequently queried columns
# MAGIC    - Avoid small files and partition explosion
# MAGIC    - Use coalesce for reducing partitions
# MAGIC 
# MAGIC 2. **Memory Management**
# MAGIC    - Cache strategically used datasets
# MAGIC    - Monitor GC pressure
# MAGIC    - Tune memory fractions for workload
# MAGIC 
# MAGIC 3. **Query Optimization**
# MAGIC    - Enable adaptive query execution
# MAGIC    - Use appropriate file formats (Delta/Parquet)
# MAGIC    - Optimize join strategies

# COMMAND ----------

# Clean up resources
df = None if 'df' in locals() else None

print("=== Cluster Management Deep Dive Complete ===")
print("\nKey Takeaways:")
print("1. Choose appropriate cluster types for different workloads")
print("2. Implement auto-scaling and cost optimization strategies")  
print("3. Monitor performance and troubleshoot systematically")
print("4. Use cluster policies for governance and compliance")
print("5. Optimize data partitioning and memory usage")
print("\nNext: Continue to 04_spark_on_databricks for distributed computing!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC 
# MAGIC ### Documentation
# MAGIC - [Cluster Configuration Best Practices](https://docs.databricks.com/clusters/cluster-config-best-practices.html)
# MAGIC - [Auto-scaling Guide](https://docs.databricks.com/clusters/configure.html#autoscaling)
# MAGIC - [Performance Tuning Guide](https://docs.databricks.com/optimizations/index.html)
# MAGIC 
# MAGIC ### Monitoring Tools
# MAGIC - Spark UI for detailed execution metrics
# MAGIC - Databricks System Tables for usage analysis
# MAGIC - CloudWatch/Azure Monitor for infrastructure metrics