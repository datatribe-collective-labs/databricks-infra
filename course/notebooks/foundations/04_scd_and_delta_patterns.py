# Databricks notebook source
# MAGIC %md
# MAGIC # Slowly Changing Dimensions (SCDs) and Delta Patterns
# MAGIC 
# MAGIC ## What are Slowly Changing Dimensions?
# MAGIC 
# MAGIC In dimensional modeling, dimension attributes can change over time:
# MAGIC - Customer moves to a new city
# MAGIC - Product price changes
# MAGIC - Employee gets promoted to a new department
# MAGIC 
# MAGIC **Slowly Changing Dimensions (SCDs)** are strategies for handling these changes while maintaining historical accuracy.
# MAGIC 
# MAGIC ## Why SCDs Matter
# MAGIC 
# MAGIC Without proper SCD handling:
# MAGIC - ❌ Historical reports become incorrect
# MAGIC - ❌ Can't answer "what was the customer's city in 2022?"
# MAGIC - ❌ Metrics change when you recalculate old reports
# MAGIC - ❌ Audit and compliance issues
# MAGIC 
# MAGIC With proper SCD handling:
# MAGIC - ✅ Historical accuracy preserved
# MAGIC - ✅ Point-in-time analysis possible
# MAGIC - ✅ Audit trail maintained
# MAGIC - ✅ Consistent reporting over time

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Types Overview
# MAGIC 
# MAGIC | Type | Strategy | Use When | Storage |
# MAGIC |------|----------|----------|---------|
# MAGIC | **Type 0** | Never change | Reference data (country codes) | Minimal |
# MAGIC | **Type 1** | Overwrite | Don't need history (typo fixes) | Minimal |
# MAGIC | **Type 2** | Add new row | Need full history (most common) | More |
# MAGIC | **Type 3** | Add new column | Need previous value only | Minimal |
# MAGIC | **Type 4** | Separate history table | Complex scenarios | Most |
# MAGIC 
# MAGIC We'll focus on **Type 1** and **Type 2** as they cover 95% of real-world cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1: Overwrite (No History)
# MAGIC 
# MAGIC **Strategy:** Simply update the existing record
# MAGIC 
# MAGIC **When to use:**
# MAGIC - Corrections (typos, data quality fixes)
# MAGIC - Changes where history doesn't matter
# MAGIC - Derived attributes that should always reflect current logic
# MAGIC 
# MAGIC **Pros:**
# MAGIC - ✅ Simple to implement
# MAGIC - ✅ No additional storage
# MAGIC - ✅ No duplicate keys
# MAGIC 
# MAGIC **Cons:**
# MAGIC - ❌ History is lost
# MAGIC - ❌ Can't do point-in-time analysis

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, current_timestamp, lit

# Initial customer dimension
initial_customers = [
    ("CUST_001", "ABC Manufacturing", "Industrial", "Helsinky", "Finland", True),  # Note: Typo in city
    ("CUST_002", "Tech Startup Oy", "Commercial", "Espoo", "Finland", True),
    ("CUST_003", "Home User", "Residential", "Vantaa", "Finland", True),
]

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("customer_type", StringType(), False),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_active", BooleanType(), True),
])

customers_df = spark.createDataFrame(initial_customers, customer_schema)

print("📋 Initial Customer Dimension")
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying SCD Type 1: Fix Typo

# COMMAND ----------

# Update to fix typo: "Helsinky" → "Helsinki"
updated_customers = [
    ("CUST_001", "ABC Manufacturing", "Industrial", "Helsinki", "Finland", True),  # Fixed!
    ("CUST_002", "Tech Startup Oy", "Commercial", "Espoo", "Finland", True),
    ("CUST_003", "Home User", "Residential", "Vantaa", "Finland", True),
]

customers_df_updated = spark.createDataFrame(updated_customers, customer_schema)

print("📋 After SCD Type 1 Update (Typo Corrected)")
display(customers_df_updated)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 1 with Delta Lake MERGE
# MAGIC 
# MAGIC In production, you'd use Delta Lake MERGE for Type 1 updates:
# MAGIC 
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC 
# MAGIC # Assume dim_customer exists as Delta table
# MAGIC delta_customer = DeltaTable.forPath(spark, "/path/to/dim_customer")
# MAGIC 
# MAGIC # Merge logic for SCD Type 1
# MAGIC delta_customer.alias("target").merge(
# MAGIC     updates_df.alias("source"),
# MAGIC     "target.customer_id = source.customer_id"
# MAGIC ).whenMatchedUpdate(
# MAGIC     set = {
# MAGIC         "customer_name": "source.customer_name",
# MAGIC         "city": "source.city",
# MAGIC         "country": "source.country",
# MAGIC         "updated_at": "current_timestamp()"
# MAGIC     }
# MAGIC ).whenNotMatchedInsert(
# MAGIC     values = {
# MAGIC         "customer_id": "source.customer_id",
# MAGIC         "customer_name": "source.customer_name",
# MAGIC         "customer_type": "source.customer_type",
# MAGIC         "city": "source.city",
# MAGIC         "country": "source.country",
# MAGIC         "is_active": "source.is_active",
# MAGIC         "created_at": "current_timestamp()"
# MAGIC     }
# MAGIC ).execute()
# MAGIC ```
# MAGIC 
# MAGIC **Result:** Record updated in-place, no history kept.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2: Add New Row (Full History)
# MAGIC 
# MAGIC **Strategy:** Keep all versions, add validity dates
# MAGIC 
# MAGIC **When to use:**
# MAGIC - Need complete historical tracking
# MAGIC - Regulatory/compliance requirements
# MAGIC - Point-in-time reporting needed
# MAGIC - Most common for customer, product, location changes
# MAGIC 
# MAGIC **Pros:**
# MAGIC - ✅ Complete history preserved
# MAGIC - ✅ Point-in-time queries possible
# MAGIC - ✅ Audit trail maintained
# MAGIC 
# MAGIC **Cons:**
# MAGIC - ❌ More storage required
# MAGIC - ❌ Queries need to handle multiple versions
# MAGIC - ❌ Surrogate keys recommended

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 Schema Design
# MAGIC 
# MAGIC Key columns for Type 2:
# MAGIC - `customer_key` (surrogate key) - Unique for each version
# MAGIC - `customer_id` (business key) - Same across versions
# MAGIC - `valid_from` - When this version became active
# MAGIC - `valid_to` - When this version expired (NULL = current)
# MAGIC - `is_current` - Boolean flag for current version

# COMMAND ----------

from pyspark.sql.types import DateType

# SCD Type 2 customer dimension with history
scd2_customers = [
    # Customer CUST_001 - Initial record
    (1, "CUST_001", "ABC Manufacturing", "Industrial", "Helsinki", "Finland", "2020-01-15", "2023-06-30", False),
    # Customer CUST_001 - Moved to Espoo
    (2, "CUST_001", "ABC Manufacturing", "Industrial", "Espoo", "Finland", "2023-07-01", None, True),
    
    # Customer CUST_002 - Initial record  
    (3, "CUST_002", "Tech Startup Oy", "Commercial", "Espoo", "Finland", "2021-06-20", "2023-11-01", False),
    # Customer CUST_002 - Changed to Enterprise type
    (4, "CUST_002", "Tech Startup Oy", "Enterprise", "Espoo", "Finland", "2023-11-01", None, True),
    
    # Customer CUST_003 - Only one version (never changed)
    (5, "CUST_003", "Home User", "Residential", "Vantaa", "Finland", "2022-03-10", None, True),
]

scd2_schema = StructType([
    StructField("customer_key", IntegerType(), False),  # Surrogate key
    StructField("customer_id", StringType(), False),    # Business key
    StructField("customer_name", StringType(), False),
    StructField("customer_type", StringType(), False),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("valid_from", StringType(), True),
    StructField("valid_to", StringType(), True),  # NULL = current
    StructField("is_current", BooleanType(), False),
])

dim_customer_scd2 = spark.createDataFrame(scd2_customers, scd2_schema)

print("📋 SCD Type 2 Customer Dimension (With History)")
display(dim_customer_scd2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying SCD Type 2: Current Records Only

# COMMAND ----------

# Get only current records
current_customers = dim_customer_scd2.filter(col("is_current") == True)

print("📋 Current Customers Only")
display(current_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying SCD Type 2: Point-in-Time Analysis

# COMMAND ----------

from pyspark.sql.functions import to_date

# What did the data look like on 2023-08-15?
point_in_time_date = "2023-08-15"

historical_customers = dim_customer_scd2.filter(
    (col("valid_from") <= point_in_time_date) &
    ((col("valid_to").isNull()) | (col("valid_to") >= point_in_time_date))
)

print(f"📋 Customer Dimension as of {point_in_time_date}")
display(historical_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notice the Results:
# MAGIC 
# MAGIC - CUST_001 shows **Espoo** (moved on 2023-07-01, so Espoo was correct on 2023-08-15)
# MAGIC - CUST_002 shows **Commercial** (changed to Enterprise on 2023-11-01, so still Commercial in August)
# MAGIC - CUST_003 unchanged (only one version exists)
# MAGIC 
# MAGIC **This is the power of SCD Type 2!** Historical accuracy preserved.

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 History: See All Changes

# COMMAND ----------

# View complete history for a specific customer
customer_history = dim_customer_scd2.filter(col("customer_id") == "CUST_001").orderBy("valid_from")

print("📋 Complete History for Customer CUST_001")
display(customer_history)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implementing SCD Type 2 with Delta Lake MERGE
# MAGIC 
# MAGIC Delta Lake makes SCD Type 2 implementation powerful with MERGE:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC from pyspark.sql.functions import current_date, lit, col
# MAGIC 
# MAGIC # Assume incoming changes
# MAGIC incoming_changes = spark.createDataFrame([
# MAGIC     ("CUST_001", "ABC Manufacturing", "Industrial", "Tampere", "Finland"),  # City changed
# MAGIC     ("CUST_004", "New Customer", "Residential", "Turku", "Finland"),       # New customer
# MAGIC ], ["customer_id", "customer_name", "customer_type", "city", "country"])
# MAGIC 
# MAGIC # Step 1: Close out old records (set valid_to and is_current)
# MAGIC dim_customer_scd2.alias("target").merge(
# MAGIC     incoming_changes.alias("source"),
# MAGIC     """target.customer_id = source.customer_id 
# MAGIC        AND target.is_current = true
# MAGIC        AND (target.city != source.city OR target.customer_type != source.customer_type)"""
# MAGIC ).whenMatchedUpdate(
# MAGIC     set = {
# MAGIC         "valid_to": "current_date()",
# MAGIC         "is_current": "false"
# MAGIC     }
# MAGIC ).execute()
# MAGIC 
# MAGIC # Step 2: Insert new versions
# MAGIC new_versions = incoming_changes.withColumn("valid_from", current_date()) \
# MAGIC     .withColumn("valid_to", lit(None).cast("date")) \
# MAGIC     .withColumn("is_current", lit(True))
# MAGIC 
# MAGIC dim_customer_scd2.alias("target").merge(
# MAGIC     new_versions.alias("source"),
# MAGIC     "target.customer_id = source.customer_id AND target.is_current = true"
# MAGIC ).whenNotMatchedInsert(
# MAGIC     values = {
# MAGIC         "customer_key": "monotonically_increasing_id()",
# MAGIC         "customer_id": "source.customer_id",
# MAGIC         "customer_name": "source.customer_name",
# MAGIC         "customer_type": "source.customer_type",
# MAGIC         "city": "source.city",
# MAGIC         "country": "source.country",
# MAGIC         "valid_from": "source.valid_from",
# MAGIC         "valid_to": "source.valid_to",
# MAGIC         "is_current": "source.is_current"
# MAGIC     }
# MAGIC ).execute()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Time Travel: Alternative to SCD Type 2
# MAGIC 
# MAGIC Delta Lake's built-in time travel can sometimes replace SCD Type 2:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Query table as it was at a specific timestamp
# MAGIC historical_df = spark.read.format("delta") \
# MAGIC     .option("timestampAsOf", "2023-08-15 10:00:00") \
# MAGIC     .load("/path/to/dim_customer")
# MAGIC 
# MAGIC # Or query by version number
# MAGIC historical_df = spark.read.format("delta") \
# MAGIC     .option("versionAsOf", 5) \
# MAGIC     .load("/path/to/dim_customer")
# MAGIC ```
# MAGIC 
# MAGIC ### When to use Delta Time Travel vs SCD Type 2:
# MAGIC 
# MAGIC **Delta Time Travel:**
# MAGIC - ✅ Simple Type 1 dimensions with occasional rollback needs
# MAGIC - ✅ Short-term history (30-90 days)
# MAGIC - ✅ Ad-hoc debugging and analysis
# MAGIC - ❌ Long-term compliance (retention policies may purge history)
# MAGIC - ❌ Complex point-in-time joins across multiple tables
# MAGIC 
# MAGIC **SCD Type 2:**
# MAGIC - ✅ Long-term historical tracking (years)
# MAGIC - ✅ Compliance and audit requirements
# MAGIC - ✅ Complex point-in-time analysis across many tables
# MAGIC - ✅ Need to query "as of" dates frequently
# MAGIC - ❌ More complex to implement and maintain

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 3: Add Previous Value Column (Rarely Used)
# MAGIC 
# MAGIC **Strategy:** Add columns for previous value
# MAGIC 
# MAGIC **Example Schema:**
# MAGIC ```
# MAGIC customer_id | name | current_city | previous_city | city_change_date
# MAGIC CUST_001    | ABC  | Espoo        | Helsinki      | 2023-07-01
# MAGIC ```
# MAGIC 
# MAGIC **When to use:**
# MAGIC - Only need to track one previous value
# MAGIC - Limited history requirements
# MAGIC - Simple before/after comparisons
# MAGIC 
# MAGIC **Why rarely used:**
# MAGIC - Only tracks one change
# MAGIC - Schema changes needed for each tracked attribute
# MAGIC - Type 2 is usually better

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table Considerations with SCD Type 2
# MAGIC 
# MAGIC ### Challenge: Which dimension version to use?
# MAGIC 
# MAGIC When joining facts to SCD Type 2 dimensions, you need the right version.

# COMMAND ----------

# Sample fact table
fact_orders = [
    (1, "CUST_001", "2023-05-15", 1500.0),  # Before CUST_001 moved
    (2, "CUST_001", "2023-08-20", 2000.0),  # After CUST_001 moved
    (3, "CUST_002", "2023-10-10", 800.0),   # Before CUST_002 became Enterprise
    (4, "CUST_002", "2023-11-15", 1200.0),  # After CUST_002 became Enterprise
]

fact_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_date", StringType(), False),
    StructField("order_amount", DoubleType(), False),
])

fact_orders_df = spark.createDataFrame(fact_orders, fact_schema)

print("📊 Fact Table: Orders")
display(fact_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1: Join on business key + date range

# COMMAND ----------

# Join fact to the correct dimension version based on date
result = fact_orders_df.alias("f") \
    .join(
        dim_customer_scd2.alias("d"),
        (col("f.customer_id") == col("d.customer_id")) &
        (col("f.order_date") >= col("d.valid_from")) &
        ((col("d.valid_to").isNull()) | (col("f.order_date") <= col("d.valid_to")))
    ) \
    .select(
        "f.order_id",
        "f.order_date",
        "d.customer_name",
        "d.customer_type",
        "d.city",
        "f.order_amount"
    ) \
    .orderBy("f.order_date")

print("📊 Orders with Historically Accurate Customer Data")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notice:
# MAGIC - Order on 2023-05-15: CUST_001 shows **Helsinki** (correct for that date)
# MAGIC - Order on 2023-08-20: CUST_001 shows **Espoo** (moved on 2023-07-01)
# MAGIC - Order on 2023-10-10: CUST_002 shows **Commercial** (before upgrade)
# MAGIC - Order on 2023-11-15: CUST_002 shows **Enterprise** (after upgrade)
# MAGIC 
# MAGIC **Historical accuracy maintained!**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2: Store surrogate key in fact table (Better)

# COMMAND ----------

# MAGIC %md
# MAGIC In production, facts should store the `customer_key` (surrogate), not `customer_id`:
# MAGIC 
# MAGIC ```python
# MAGIC # Fact table with surrogate key
# MAGIC fact_orders: order_id, customer_key, order_date, amount
# MAGIC 
# MAGIC # Direct join - no date logic needed!
# MAGIC result = fact_orders.join(dim_customer_scd2, "customer_key")
# MAGIC ```
# MAGIC 
# MAGIC **Benefits:**
# MAGIC - ✅ Simple joins (just match on key)
# MAGIC - ✅ Faster queries (no date range filtering)
# MAGIC - ✅ Automatically point-in-time correct

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing the Right SCD Type
# MAGIC 
# MAGIC | Scenario | Recommended Type | Reason |
# MAGIC |----------|-----------------|--------|
# MAGIC | Typo corrections | Type 1 | No need for incorrect history |
# MAGIC | Email updates | Type 1 | Old email irrelevant |
# MAGIC | Customer address | Type 2 | Need historical location data |
# MAGIC | Employee department | Type 2 | Org reporting needs history |
# MAGIC | Product category | Type 2 | Historical analysis critical |
# MAGIC | Derived flags | Type 1 | Should reflect current logic |
# MAGIC | Price changes | Type 2 + Type 4 | Complex, might need price history table |
# MAGIC | Status changes | Type 2 | Full audit trail needed |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices for SCDs
# MAGIC 
# MAGIC ### 1. Document Your SCD Strategy
# MAGIC ```python
# MAGIC # Add comments to schema
# MAGIC """
# MAGIC dim_customer:
# MAGIC   - customer_id: SCD Type 2 (track all changes)
# MAGIC   - customer_name: SCD Type 1 (corrections only)
# MAGIC   - email: SCD Type 1 (current contact only)
# MAGIC   - city: SCD Type 2 (historical location matters)
# MAGIC   - loyalty_tier: SCD Type 2 (reporting needs history)
# MAGIC """
# MAGIC ```
# MAGIC 
# MAGIC ### 2. Use Consistent Naming
# MAGIC ```python
# MAGIC # Standard SCD Type 2 columns:
# MAGIC valid_from, valid_to, is_current
# MAGIC 
# MAGIC # Or alternatively:
# MAGIC effective_from, effective_to, is_active
# MAGIC 
# MAGIC # Pick one convention and stick to it!
# MAGIC ```
# MAGIC 
# MAGIC ### 3. Index Appropriately
# MAGIC ```python
# MAGIC # For SCD Type 2, index on:
# MAGIC # - Business key (customer_id)
# MAGIC # - is_current flag
# MAGIC # - valid_from, valid_to dates
# MAGIC ```
# MAGIC 
# MAGIC ### 4. Add Data Quality Checks
# MAGIC ```python
# MAGIC # Ensure data quality:
# MAGIC assert dim_customer.filter("is_current = true").groupBy("customer_id").count()
# MAGIC     .filter("count > 1").count() == 0, "Multiple current records found!"
# MAGIC 
# MAGIC assert dim_customer.filter("valid_from > valid_to").count() == 0, 
# MAGIC     "Invalid date ranges found!"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Features for SCD Management
# MAGIC 
# MAGIC ### 1. MERGE for Upserts
# MAGIC Perfect for SCD Type 1 and Type 2 implementation
# MAGIC 
# MAGIC ### 2. Time Travel
# MAGIC Alternative to SCD Type 2 for short-term history
# MAGIC 
# MAGIC ### 3. Change Data Feed
# MAGIC Track all changes automatically
# MAGIC ```python
# MAGIC # Enable change data feed
# MAGIC spark.sql("""
# MAGIC   ALTER TABLE dim_customer 
# MAGIC   SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC """)
# MAGIC 
# MAGIC # Read changes
# MAGIC changes = spark.read.format("delta") \
# MAGIC   .option("readChangeData", "true") \
# MAGIC   .option("startingVersion", 0) \
# MAGIC   .table("dim_customer")
# MAGIC ```
# MAGIC 
# MAGIC ### 4. Schema Evolution
# MAGIC Add SCD columns without breaking existing pipelines
# MAGIC ```python
# MAGIC .option("mergeSchema", "true")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-World Example: Complete SCD Type 2 Pipeline
# MAGIC 
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC from pyspark.sql.functions import current_timestamp, col, lit
# MAGIC 
# MAGIC def apply_scd_type2(delta_table_path, incoming_df, business_key, compare_cols):
# MAGIC     """
# MAGIC     Apply SCD Type 2 logic to a Delta table
# MAGIC     
# MAGIC     Args:
# MAGIC         delta_table_path: Path to Delta table
# MAGIC         incoming_df: New/updated records
# MAGIC         business_key: Business key column (e.g., 'customer_id')
# MAGIC         compare_cols: Columns to compare for changes (e.g., ['city', 'customer_type'])
# MAGIC     """
# MAGIC     
# MAGIC     # Load existing dimension
# MAGIC     dim_table = DeltaTable.forPath(spark, delta_table_path)
# MAGIC     
# MAGIC     # Step 1: Identify changed records
# MAGIC     change_condition = " OR ".join([
# MAGIC         f"target.{col} != source.{col}" for col in compare_cols
# MAGIC     ])
# MAGIC     
# MAGIC     # Step 2: Close out old versions
# MAGIC     dim_table.alias("target").merge(
# MAGIC         incoming_df.alias("source"),
# MAGIC         f"target.{business_key} = source.{business_key} AND target.is_current = true AND ({change_condition})"
# MAGIC     ).whenMatchedUpdate(
# MAGIC         set = {
# MAGIC             "valid_to": "current_date()",
# MAGIC             "is_current": "false",
# MAGIC             "updated_at": "current_timestamp()"
# MAGIC         }
# MAGIC     ).execute()
# MAGIC     
# MAGIC     # Step 3: Insert new versions (changed records + new records)
# MAGIC     new_versions = incoming_df \
# MAGIC         .withColumn("valid_from", current_date()) \
# MAGIC         .withColumn("valid_to", lit(None).cast("date")) \
# MAGIC         .withColumn("is_current", lit(True)) \
# MAGIC         .withColumn("created_at", current_timestamp())
# MAGIC     
# MAGIC     dim_table.alias("target").merge(
# MAGIC         new_versions.alias("source"),
# MAGIC         f"target.{business_key} = source.{business_key} AND target.is_current = true"
# MAGIC     ).whenNotMatchedInsert(
# MAGIC         values = {col_name: f"source.{col_name}" for col_name in new_versions.columns}
# MAGIC     ).execute()
# MAGIC 
# MAGIC # Usage
# MAGIC apply_scd_type2(
# MAGIC     delta_table_path="/mnt/delta/dim_customer",
# MAGIC     incoming_df=new_customer_data,
# MAGIC     business_key="customer_id",
# MAGIC     compare_cols=["customer_name", "city", "customer_type"]
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Key Takeaways
# MAGIC 
# MAGIC - **SCD Type 1**: Overwrite for corrections, no history needed
# MAGIC - **SCD Type 2**: Add new rows for full history (most common)
# MAGIC - **SCD Type 3**: Add columns for previous value (rarely used)
# MAGIC - Use `valid_from`, `valid_to`, `is_current` for Type 2
# MAGIC - Delta Lake MERGE makes SCD implementation efficient
# MAGIC - Delta Time Travel can replace Type 2 for short-term needs
# MAGIC - Store surrogate keys in facts for easier joins
# MAGIC - Point-in-time analysis requires careful date-based joins
# MAGIC - Document your SCD strategy for each dimension attribute
# MAGIC - Use Delta Lake features: MERGE, Time Travel, Change Data Feed
# MAGIC 
# MAGIC ## What's Next?
# MAGIC 
# MAGIC You now have the conceptual foundation for:
# MAGIC - Building properly modeled data (Introduction)
# MAGIC - Organizing data in layers (Medallion Architecture)
# MAGIC - Designing facts and dimensions (Dimensional Modeling)
# MAGIC - Handling changes over time (SCDs)
# MAGIC 
# MAGIC In the advanced section of this course, you'll apply these concepts hands-on with real Databricks implementations using Delta Lake, dbt, and orchestration!
