# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Concepts Explained - Week 01
# MAGIC
# MAGIC This notebook provides a practical, concept-focused exploration of Delta Lake fundamentals.
# MAGIC Each section pairs a logical concept with hands-on examples to build solid understanding.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand what Delta Lake is and why it matters
# MAGIC - Learn ACID properties with practical examples
# MAGIC - Explore the Delta transaction log structure
# MAGIC - See how ACID properties map to Delta log transactions
# MAGIC - Work with real data to solidify understanding
# MAGIC
# MAGIC ## Topics Covered
# MAGIC
# MAGIC 1. What is a Delta Table?
# MAGIC 2. ACID Properties Explained
# MAGIC 3. Delta Transaction Log Deep Dive
# MAGIC 4. ACID in Action: Practical Examples
# MAGIC 5. Delta Log Contents and Structure

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is a Delta Table?
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC A Delta table is not just data files - it's a complete data management system consisting of:
# MAGIC - **Parquet data files**: Actual data stored in columnar format
# MAGIC - **Transaction log (_delta_log)**: JSON files tracking all changes
# MAGIC - **Metadata**: Schema, partitioning info, table properties
# MAGIC
# MAGIC ### Why Delta Lake?
# MAGIC
# MAGIC Traditional data lakes have problems:
# MAGIC - ❌ No transactions → data corruption possible
# MAGIC - ❌ No schema enforcement → bad data gets in
# MAGIC - ❌ Hard to update/delete → only append operations
# MAGIC - ❌ No time travel → can't see historical data
# MAGIC
# MAGIC Delta Lake solves these:
# MAGIC - ✅ ACID transactions → reliable operations
# MAGIC - ✅ Schema enforcement → data quality guaranteed
# MAGIC - ✅ Update/Delete/Merge → full DML support
# MAGIC - ✅ Time travel → query any historical version

# COMMAND ----------

# Let's create our first Delta table and examine its structure
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

print("=== Creating Your First Delta Table ===\n")

# Define explicit schema (best practice)
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("total_purchases", DoubleType(), True),
    StructField("created_at", TimestampType(), False)
])

# Create sample customer data
customers_data = [
    (1, "Alice Johnson", "alice@example.com", 1250.50, datetime(2024, 1, 15, 10, 30)),
    (2, "Bob Smith", "bob@example.com", 890.25, datetime(2024, 1, 16, 14, 20)),
    (3, "Carol White", "carol@example.com", 2100.75, datetime(2024, 1, 17, 9, 15))
]

# Create DataFrame
df_customers = spark.createDataFrame(customers_data, schema)

print("Sample customer data:")
df_customers.show(truncate=False)

# Write as Delta table

import re

# Re-defining the course schema for write access
CATALOG = "databricks_course" 
# Get logged in user's username
USER_SCHEMA_RAW = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# Remove latter part of email address, and replace special characters with underscore to avoid SQL parsing errors
USER_SCHEMA = re.sub(r'[^a-zA-Z0-9_]', '_', USER_SCHEMA_RAW.split('@')[0])
# If schema didn't exist before, now it is being created
VOLUME_NAME = "scratch"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{USER_SCHEMA}.{VOLUME_NAME}")

# The POSIX-style path for your Spark writes
VOLUME_PATH = f"/Volumes/{CATALOG}/{USER_SCHEMA}/{VOLUME_NAME}/"
print(f"Your Writable Volume Path is: {VOLUME_PATH}")

try:
    delta_path = f"{VOLUME_PATH}delta_customers/customers"
    df_customers.write.format("delta").mode("overwrite").save(delta_path)  

except Exception as e:
    print(f"Writing to Delta table at {delta_path} not supported: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ACID Properties Explained
# MAGIC
# MAGIC ACID is an acronym for four critical properties that guarantee reliable database transactions:
# MAGIC
# MAGIC ### A - Atomicity
# MAGIC **Concept**: All or nothing - either the entire operation succeeds or nothing changes.
# MAGIC
# MAGIC **Real-world analogy**: Bank transfer - money must leave one account AND enter another, or neither happens.
# MAGIC
# MAGIC ### C - Consistency
# MAGIC **Concept**: Data must always follow defined rules (schema, constraints).
# MAGIC
# MAGIC **Real-world analogy**: Age field must be a positive number, email must be valid format.
# MAGIC
# MAGIC ### I - Isolation
# MAGIC **Concept**: Concurrent operations don't interfere with each other.
# MAGIC
# MAGIC **Real-world analogy**: Two people withdrawing from the same account simultaneously don't cause corruption.
# MAGIC
# MAGIC ### D - Durability
# MAGIC **Concept**: Once committed, data survives system failures.
# MAGIC
# MAGIC **Real-world analogy**: After you see "Transaction successful", a power outage won't lose your data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACID Property #1: Atomicity in Action

# COMMAND ----------

from pyspark.sql.functions import col

print("=== Demonstrating ATOMICITY ===\n")

# Scenario: We want to add 3 new customers
# What happens if something goes wrong mid-operation?

new_customers = [
    (4, "David Brown", "david@example.com", 450.00, datetime.now()),
    (5, "Eve Davis", "eve@example.com", 890.50, datetime.now()),
    (6, "Frank Miller", "frank@example.com", 1200.00, datetime.now())
]

df_new = spark.createDataFrame(new_customers, schema)

print("Before insertion:")
df_before = spark.read.format("delta").load(delta_path)
print(f"Customer count: {df_before.count()}")

# Insert new customers (atomic operation)
df_new.write.format("delta").mode("append").save(delta_path)

print("\nAfter insertion:")
df_after = spark.read.format("delta").load(delta_path)
print(f"Customer count: {df_after.count()}")

print("\n✅ Key Point: Either all 3 customers are inserted, or none are.")
print("   Delta guarantees you'll never see partial results (e.g., only 2 customers)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACID Property #2: Consistency in Action

# COMMAND ----------

print("=== Demonstrating CONSISTENCY ===\n")

print("Current table schema:")
df_current = spark.read.format("delta").load(delta_path)
df_current.printSchema()

# Try to insert data with wrong schema (should fail or be rejected)
print("\nAttempt 1: Try to insert data with missing required field")
try:
    # Missing customer_id (required field)
    bad_data = [(None, "Bad User", "bad@example.com", 100.0, datetime.now())]
    df_bad = spark.createDataFrame(bad_data, schema)
    df_bad.write.format("delta").mode("append").save(delta_path)
    print("❌ This shouldn't succeed!")
except Exception as e:
    print(f"✅ Rejected as expected: {type(e).__name__}")

print("\nAttempt 2: Try to insert data with wrong data type")
# Schema enforcement: Delta will validate types
wrong_schema_data = [
    ("not_an_int", "Wrong Type", "wrong@example.com", "not_a_float", datetime.now())
]

try:
    # This will fail because customer_id should be IntegerType
    df_wrong = spark.createDataFrame(wrong_schema_data, ["customer_id", "customer_name", "email", "total_purchases", "created_at"])
    df_wrong.write.format("delta").mode("append").save(delta_path)
    print("❌ This shouldn't succeed!")
except Exception as e:
    print(f"✅ Type validation works: {type(e).__name__}")

print("\n✅ Key Point: Delta enforces schema consistency.")
print("   Bad data cannot corrupt your table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACID Property #3: Isolation in Action

# COMMAND ----------

print("=== Demonstrating ISOLATION ===\n")

# Simulate concurrent operations (in practice these would be separate jobs/users)
print("Scenario: Two operations happen simultaneously")
print("  - Operation A: Reading customer data for reporting")
print("  - Operation B: Adding new customers")

# Operation A: Start reading
print("\nOperation A: Reading data...")
df_read = spark.read.format("delta").load(delta_path)
count_during_read = df_read.count()
print(f"  Snapshot shows {count_during_read} customers")

# Operation B: Writing new data (happens "during" the read)
print("\nOperation B: Inserting new customer...")
new_customer = [(7, "Grace Lee", "grace@example.com", 350.00, datetime.now())]
df_new_customer = spark.createDataFrame(new_customer, schema)
df_new_customer.write.format("delta").mode("append").save(delta_path)
print("  New customer inserted")

# Operation A: Continue with original snapshot
print("\nOperation A: Completing analysis on original snapshot...")
print(f"  Still sees {count_during_read} customers (isolated from Operation B)")

# New read: See the updated data
df_latest = spark.read.format("delta").load(delta_path)
print(f"\nNew read: Now sees {df_latest.count()} customers")

print("\n✅ Key Point: Readers see consistent snapshots.")
print("   Writers don't interfere with ongoing reads (snapshot isolation)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACID Property #4: Durability in Action

# COMMAND ----------

print("=== Demonstrating DURABILITY ===\n")

# Make a critical update
print("Making an important update to customer records...")

df_update = spark.read.format("delta").load(delta_path)
df_updated = df_update.withColumn(
    "total_purchases",
    col("total_purchases") * 1.1  # 10% loyalty bonus
)

df_updated.write.format("delta").mode("overwrite").save(delta_path)
print("✅ Update committed successfully")

# Verify the change persisted
df_verify = spark.read.format("delta").load(delta_path)
print("\nVerifying data persistence:")
df_verify.select("customer_id", "customer_name", "total_purchases").show(5)

print("\n✅ Key Point: Once committed, this data survives:")
print("   - Cluster restarts")
print("   - System crashes")
print("   - Power failures")
print("   The transaction log ensures durability")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Transaction Log Deep Dive
# MAGIC
# MAGIC ### What is the Transaction Log?
# MAGIC
# MAGIC The `_delta_log` directory is the "single source of truth" for a Delta table.
# MAGIC
# MAGIC **Structure:**
# MAGIC ```
# MAGIC /path/to/delta_table/
# MAGIC ├── _delta_log/
# MAGIC │   ├── 00000000000000000000.json  ← Version 0 (initial table creation)
# MAGIC │   ├── 00000000000000000001.json  ← Version 1 (first change)
# MAGIC │   ├── 00000000000000000002.json  ← Version 2 (second change)
# MAGIC │   ├── 00000000000000000010.checkpoint.parquet ← Checkpoint (every 10 versions)
# MAGIC │   └── _last_checkpoint                        ← Pointer to latest checkpoint
# MAGIC ├── part-00000-xxx.snappy.parquet  ← Actual data file
# MAGIC └── part-00001-xxx.snappy.parquet  ← Another data file
# MAGIC ```
# MAGIC
# MAGIC **Each JSON log file contains:**
# MAGIC - Protocol version
# MAGIC - Metadata (schema, partitioning)
# MAGIC - Add actions (new files added)
# MAGIC - Remove actions (files marked as deleted)
# MAGIC - Transaction info (timestamps, operation type)

# COMMAND ----------

# Let's examine the actual Delta log
print("=== Exploring the Delta Transaction Log ===\n")

# List files in the delta directory
import subprocess
result = subprocess.run(['ls', '-lah', delta_path], capture_output=True, text=True)
print("Delta table directory contents:")
print(result.stdout)

# Look at the _delta_log directory
log_path = f"{delta_path}/_delta_log"
result_log = subprocess.run(['ls', '-lah', log_path], capture_output=True, text=True)
print("\n_delta_log directory contents:")
print(result_log.stdout)

# Count the number of versions
result_count = subprocess.run(['ls', log_path], capture_output=True, text=True)
json_files = [f for f in result_count.stdout.split('\n') if f.endswith('.json')]
print(f"\nNumber of transaction log versions: {len(json_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the Transaction Log

# COMMAND ----------

import json

print("=== Examining Transaction Log Contents ===\n")

# Read the first transaction log file (version 0)
log_file_0 = f"{delta_path}/_delta_log/00000000000000000000.json"

try:
    with open(log_file_0.replace('file:', ''), 'r') as f:
        # Read all lines (each line is a separate JSON object)
        log_entries = [json.loads(line) for line in f]

    print(f"Version 0 contains {len(log_entries)} log entries\n")

    # Show each type of entry
    for idx, entry in enumerate(log_entries[:5]):  # Show first 5 entries
        print(f"Entry {idx}:")
        if 'protocol' in entry:
            print(f"  Type: Protocol")
            print(f"  Min Reader Version: {entry['protocol'].get('minReaderVersion')}")
            print(f"  Min Writer Version: {entry['protocol'].get('minWriterVersion')}")

        elif 'metaData' in entry:
            print(f"  Type: Metadata")
            print(f"  Schema Fields: {len(entry['metaData'].get('schemaString', '{}'))}")
            print(f"  Partition Columns: {entry['metaData'].get('partitionColumns', [])}")
            print(f"  Format: {entry['metaData'].get('format', {}).get('provider')}")

        elif 'add' in entry:
            print(f"  Type: Add File")
            print(f"  Path: {entry['add']['path'][:50]}...")
            print(f"  Size: {entry['add']['size']} bytes")
            print(f"  Modification Time: {entry['add']['modificationTime']}")

        elif 'remove' in entry:
            print(f"  Type: Remove File")
            print(f"  Path: {entry['remove']['path'][:50]}...")

        print()

except Exception as e:
    print(f"Could not read log file directly: {e}")
    print("This is expected in some Databricks environments")

# COMMAND ----------

# Use Delta's API to examine history
print("=== Delta Table History (via API) ===\n")

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_path)
history_df = delta_table.history()

print("Transaction history:")
history_df.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

print(f"\nTotal versions: {history_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ACID and Delta Log: The Connection
# MAGIC
# MAGIC Let's see exactly how ACID properties are implemented through the Delta log.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Atomicity Through Log Commits

# COMMAND ----------

print("=== How Atomicity Works with Delta Log ===\n")

print("Current version:")
current_version = delta_table.history().select("version").first()[0]
print(f"Version: {current_version}")

# Perform an update operation
print("\nPerforming update operation (increase purchases by 5%)...")

df_to_update = spark.read.format("delta").load(delta_path)
df_updated = df_to_update.withColumn("total_purchases", col("total_purchases") * 1.05)

# Write the update
df_updated.write.format("delta").mode("overwrite").save(delta_path)

print("\nAfter update:")
new_version = DeltaTable.forPath(spark, delta_path).history().select("version").first()[0]
print(f"Version: {new_version}")

# What happened in the log?
print("\nDelta Log Perspective:")
print("  1. Delta creates a new JSON file (version N+1)")
print("  2. Writes 'remove' actions for old data files")
print("  3. Writes 'add' actions for new data files")
print("  4. Only when ALL writes complete, the version file is committed")
print("  5. If any step fails, the version file isn't created = rollback")

history_latest = delta_table.history().limit(2)
print("\nLast 2 operations:")
history_latest.select("version", "operation", "operationMetrics").show(truncate=False)

print("\n✅ This is atomicity: The log commit is atomic")
print("   Either version N+1 exists (success) or it doesn't (failure)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Consistency Through Schema Evolution

# COMMAND ----------

print("=== How Consistency Works with Delta Log ===\n")

# Current schema is stored in the log
print("Current schema (from metadata in log):")
spark.read.format("delta").load(delta_path).printSchema()

# Try to add a new column with schema evolution
print("\nAttempt: Add 'membership_tier' column")

df_with_new_col = spark.read.format("delta").load(delta_path).withColumn("membership_tier", lit("Gold"))

# This requires schema evolution option
try:
    df_with_new_col.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_path)
    print("✅ Schema evolution successful (with mergeSchema=true)")
except Exception as e:
    print(f"❌ Would fail without mergeSchema option: {e}")

# Verify new schema
print("\nNew schema:")
spark.read.format("delta").load(delta_path).printSchema()

# What happened in the log?
print("\nDelta Log Perspective:")
print("  1. New metadata entry written to version N+1")
print("  2. Metadata contains updated schema with new column")
print("  3. All future reads use this schema")
print("  4. Schema is enforced for all future writes")

print("\n✅ Consistency maintained: Schema changes are tracked and enforced")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Isolation Through Versioned Reads

# COMMAND ----------

print("=== How Isolation Works with Delta Log ===\n")

# Get current version
current_version = DeltaTable.forPath(spark, delta_path).history().select("version").first()[0]
print(f"Current table version: {current_version}")

# Read at a specific version (snapshot isolation)
print(f"\nRead 1: Reading current version ({current_version})")
df_snapshot1 = spark.read.format("delta").load(delta_path)
count_snapshot1 = df_snapshot1.count()
print(f"  Record count: {count_snapshot1}")

# Make a change (add new records)
print("\nWrite operation: Adding 2 new customers")
new_batch = [
    (8, "Henry Ford", "henry@example.com", 500.00, datetime.now()),
    (9, "Iris Chen", "iris@example.com", 750.00, datetime.now())
]
df_new_batch = spark.createDataFrame(new_batch, schema)
df_new_batch.write.format("delta").mode("append").save(delta_path)

new_version = DeltaTable.forPath(spark, delta_path).history().select("version").first()[0]
print(f"  New version: {new_version}")

# Read at old version (time travel)
print(f"\nRead 2: Reading old version ({current_version}) via time travel")
df_snapshot2 = spark.read.format("delta").option("versionAsOf", current_version).load(delta_path)
count_snapshot2 = df_snapshot2.count()
print(f"  Record count: {count_snapshot2}")

# Read latest version
print(f"\nRead 3: Reading latest version ({new_version})")
df_snapshot3 = spark.read.format("delta").load(delta_path)
count_snapshot3 = df_snapshot3.count()
print(f"  Record count: {count_snapshot3}")

print("\nDelta Log Perspective:")
print(f"  - Version {current_version} log shows {count_snapshot2} records")
print(f"  - Version {new_version} log shows {count_snapshot3} records")
print("  - Multiple readers can access different versions simultaneously")
print("  - Each version is immutable in the log")

print("\n✅ Isolation achieved: Each reader sees a consistent version")
print("   Concurrent operations don't interfere")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Durability Through Log Persistence

# COMMAND ----------

print("=== How Durability Works with Delta Log ===\n")

# The transaction log provides durability
print("Durability mechanism:")
print("  1. Data files are written to storage")
print("  2. Transaction log entry is written (JSON file)")
print("  3. Log entry commit is atomic (appears instantly or not at all)")
print("  4. Once log entry exists, transaction is durable")

# Demonstrate: Even if we lose cache/memory, data is there
# print("\nClearing Spark cache...")  # Not supported on serverless, so skip

print("Reading from persistent storage...")
df_persistent = spark.read.format("delta").load(delta_path)
print(f"Record count: {df_persistent.count()}")

# Show all versions exist
print("\nAll transaction versions (durable):")
history_df = DeltaTable.forPath(spark, delta_path).history()
history_df.select("version", "timestamp", "operation").show(truncate=False)

print("\n✅ Durability guaranteed: Transaction log is the source of truth")
print("   All committed transactions survive system failures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practical Exercise: Complete ACID Workflow

# COMMAND ----------

print("=== Complete ACID Workflow Example ===")
print("Scenario: E-commerce order processing system\n")

# Create orders table
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_amount", DoubleType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_date", TimestampType(), False)
])

orders_data = [
    (1001, 1, 125.50, "pending", datetime(2024, 1, 20, 10, 0)),
    (1002, 2, 89.25, "pending", datetime(2024, 1, 20, 10, 15)),
    (1003, 3, 210.75, "pending", datetime(2024, 1, 20, 10, 30))
]

df_orders = spark.createDataFrame(orders_data, orders_schema)

try:
    orders_path = "/tmp/delta_orders/orders"
    df_orders.write.format("delta").mode("overwrite").save(orders_path)

except Exception as e:
    print(f"Writing to {orders_path} not supported: {e}")

    import re

    # Re-defining the course schema for write access
    CATALOG = "databricks_course" 
    # Get logged in user's username
    USER_SCHEMA_RAW = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    # Remove latter part of email address, and replace special characters with underscore to avoid SQL parsing errors
    USER_SCHEMA = re.sub(r'[^a-zA-Z0-9_]', '_', USER_SCHEMA_RAW.split('@')[0])
    # If schema didn't exist before, now it is being created
    VOLUME_NAME = "scratch"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{USER_SCHEMA}.{VOLUME_NAME}")

    # The POSIX-style path for your Spark writes
    VOLUME_PATH = f"/Volumes/{CATALOG}/{USER_SCHEMA}/{VOLUME_NAME}/"
    print(f"Your Writable Volume Path is: {VOLUME_PATH}")

    orders_path = f"{VOLUME_PATH}delta_orders/orders"
    df_orders.write.format("delta").mode("overwrite").save(orders_path)

print(f"✅ Orders table created at {orders_path}")
print("Initial orders:")
spark.read.format("delta").load(orders_path).show()

# COMMAND ----------

# Step 1: ATOMICITY - Process batch of orders (all or nothing)
print("\n=== Step 1: ATOMICITY - Batch Order Processing ===")

from pyspark.sql.functions import when

df_orders_current = spark.read.format("delta").load(orders_path)

# Update all pending orders to "processing"
df_orders_updated = df_orders_current.withColumn(
    "order_status",
    when(col("order_status") == "pending", "processing").otherwise(col("order_status"))
)

df_orders_updated.write.format("delta").mode("overwrite").save(orders_path)

print("✅ All orders moved to 'processing' atomically")
spark.read.format("delta").load(orders_path).show()

# COMMAND ----------

# Step 2: CONSISTENCY - Enforce business rules
print("\n=== Step 2: CONSISTENCY - Enforce Business Rules ===")

# Try to add an order with negative amount (should validate)
invalid_order = [(1004, 1, -50.00, "pending", datetime.now())]
df_invalid = spark.createDataFrame(invalid_order, orders_schema)

print("Attempting to insert order with negative amount...")

# Add validation logic
df_with_validation = df_invalid.filter(col("order_amount") > 0)

if df_with_validation.count() == 0:
    print("✅ Validation rejected invalid order (negative amount)")
else:
    df_with_validation.write.format("delta").mode("append").save(orders_path)

print("\nTable remains consistent:")
spark.read.format("delta").load(orders_path).show()

# COMMAND ----------

# Step 3: ISOLATION - Concurrent reads during writes
print("\n=== Step 3: ISOLATION - Concurrent Operations ===")

orders_table = DeltaTable.forPath(spark, orders_path)
version_before = orders_table.history().select("version").first()[0]

print(f"Version before update: {version_before}")
df_snapshot = spark.read.format("delta").option("versionAsOf", version_before).load(orders_path)
print(f"Snapshot record count: {df_snapshot.count()}")

# Add new orders while others are reading the snapshot
new_orders = [
    (1005, 4, 150.00, "pending", datetime.now()),
    (1006, 5, 200.00, "pending", datetime.now())
]
df_new_orders = spark.createDataFrame(new_orders, orders_schema)
df_new_orders.write.format("delta").mode("append").save(orders_path)

version_after = DeltaTable.forPath(spark, orders_path).history().select("version").first()[0]
print(f"\nVersion after update: {version_after}")

print("\nReading from old snapshot still works (isolation):")
df_old_snapshot = spark.read.format("delta").option("versionAsOf", version_before).load(orders_path)
print(f"Old snapshot count: {df_old_snapshot.count()}")

print("\nReading latest version shows new data:")
df_latest = spark.read.format("delta").load(orders_path)
print(f"Latest count: {df_latest.count()}")

print("✅ Isolation maintained: Readers see consistent snapshots")

# COMMAND ----------

# Step 4: DURABILITY - Verify persistence
print("\n=== Step 4: DURABILITY - Transaction Persistence ===")

# Show complete history
print("Complete transaction history (durable log):")
orders_table = DeltaTable.forPath(spark, orders_path)
orders_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Examine delta log
print("\nDelta log structure:")
import subprocess
result = subprocess.run(['ls', '-lah', f"{orders_path}/_delta_log"], capture_output=True, text=True)
print(result.stdout)

print("\n✅ All transactions persisted in durable log")
print("   System can recover from any failure using this log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### What We Learned
# MAGIC
# MAGIC 1. **Delta Table Structure**
# MAGIC    - Parquet data files + Transaction log
# MAGIC    - `_delta_log` directory is the source of truth
# MAGIC
# MAGIC 2. **ACID Properties in Practice**
# MAGIC    - **A**tomicity: All-or-nothing operations via log commits
# MAGIC    - **C**onsistency: Schema enforcement via metadata in log
# MAGIC    - **I**solation: Snapshot isolation via version numbers
# MAGIC    - **D**urability: Persistence via immutable transaction log
# MAGIC
# MAGIC 3. **Transaction Log Structure**
# MAGIC    - Protocol version entries
# MAGIC    - Metadata entries (schema, partitioning)
# MAGIC    - Add/Remove actions (file operations)
# MAGIC    - Checkpoints every 10 versions
# MAGIC
# MAGIC 4. **ACID ↔ Log Mapping**
# MAGIC    - Every operation creates a new log version
# MAGIC    - Log versions are immutable and sequential
# MAGIC    - Time travel enabled by version history
# MAGIC    - Concurrent operations isolated by versions
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - ✅ Always use explicit schemas
# MAGIC - ✅ Enable schema evolution when needed (`mergeSchema=true`)
# MAGIC - ✅ Use time travel for auditing and rollback
# MAGIC - ✅ Monitor table history regularly
# MAGIC - ✅ Understand that Delta = Data + Log (not just data)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Explore Delta Lake optimizations (OPTIMIZE, VACUUM)
# MAGIC - Learn about Z-ordering and data skipping
# MAGIC - Practice with merge operations (UPSERT)
# MAGIC - Study change data capture (CDF)

# COMMAND ----------

# Clean up example data
print("=== Cleanup ===")
print("\nExample tables created in /tmp/delta_examples/")
print("  - customers")
print("  - orders")
print("\nThese are temporary and will be cleaned up when cluster terminates")