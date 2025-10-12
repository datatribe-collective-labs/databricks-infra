# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Transformation Concepts Explained - Week 3
# MAGIC
# MAGIC This notebook provides a practical exploration of Spark transformation concepts,
# MAGIC focusing on understanding how Spark thinks and executes transformations.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand lazy evaluation and execution plans
# MAGIC - Learn narrow vs wide transformations
# MAGIC - Master partitioning and shuffling concepts
# MAGIC - Explore Catalyst optimizer internals
# MAGIC - Understand caching and persistence strategies
# MAGIC
# MAGIC ## Topics Covered
# MAGIC
# MAGIC 1. Lazy Evaluation: When and Why
# MAGIC 2. Narrow vs Wide Transformations
# MAGIC 3. Partitioning and Shuffling Explained
# MAGIC 4. Catalyst Optimizer Deep Dive
# MAGIC 5. Caching Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lazy Evaluation: When and Why
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Lazy Evaluation**: Spark doesn't execute transformations immediately - it builds a plan and executes only when an action is called.
# MAGIC
# MAGIC **Why Lazy?**
# MAGIC 1. **Optimization**: Spark can optimize the entire pipeline before execution
# MAGIC 2. **Efficiency**: Avoid unnecessary computations
# MAGIC 3. **Pipelining**: Combine multiple operations into fewer stages
# MAGIC
# MAGIC **Transformations (Lazy)**: `select`, `filter`, `join`, `groupBy`
# MAGIC **Actions (Eager)**: `show`, `count`, `collect`, `save`
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC
# MAGIC Think of lazy evaluation like planning a road trip:
# MAGIC - **Transformations**: Planning the route (turn left, go straight, stop for gas)
# MAGIC - **Actions**: Actually starting the car and driving
# MAGIC - **Optimization**: GPS finds the best route considering all your plans before you start

# COMMAND ----------

from pyspark.sql.functions import col, upper, length, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

print("=== Lazy Evaluation Demonstration ===\n")

# Create sample data
employees_data = [
    (1, "Alice Johnson", "Engineering", 95000),
    (2, "Bob Smith", "Engineering", 88000),
    (3, "Carol White", "Marketing", 72000),
    (4, "David Brown", "Engineering", 105000),
    (5, "Eve Davis", "Marketing", 78000),
    (6, "Frank Miller", "Sales", 82000)
]

schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False)
])

df_employees = spark.createDataFrame(employees_data, schema)

print("Step 1: Create DataFrame (lazy)")
print("  ⏸️  No execution yet - just metadata stored")

print("\nStep 2: Apply transformations (all lazy)")
df_transformed = df_employees \
    .filter(col("department") == "Engineering") \
    .withColumn("name_upper", upper(col("name"))) \
    .withColumn("salary_k", col("salary") / 1000) \
    .select("emp_id", "name_upper", "salary_k")

print("  ⏸️  Still no execution - building logical plan")

print("\nStep 3: Call action (triggers execution)")
print("  ▶️  NOW Spark executes everything!")
df_transformed.show()

print("\n✅ Lazy evaluation benefits:")
print("  - Spark optimized all 4 operations together")
print("  - Only read necessary columns")
print("  - Applied filter early (predicate pushdown)")

# COMMAND ----------

print("=== Viewing the Execution Plan ===\n")

print("Logical Plan (what you asked for):")
print("-" * 50)
df_transformed.explain(mode="simple")

print("\n\nPhysical Plan (how Spark will execute it):")
print("-" * 50)
df_transformed.explain(mode="formatted")

print("\n✅ Key observations:")
print("  - Filter applied early (before transformations)")
print("  - Column pruning (only reads needed columns)")
print("  - Operations combined into single pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Narrow vs Wide Transformations
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Narrow Transformations:**
# MAGIC - Each input partition contributes to **one** output partition
# MAGIC - No data shuffling across partitions
# MAGIC - Fast and efficient
# MAGIC - Examples: `map`, `filter`, `select`, `withColumn`
# MAGIC
# MAGIC **Wide Transformations:**
# MAGIC - Each input partition contributes to **multiple** output partitions
# MAGIC - Requires data shuffling (expensive!)
# MAGIC - Creates stage boundaries
# MAGIC - Examples: `groupBy`, `join`, `orderBy`, `distinct`
# MAGIC
# MAGIC ### Visual Representation
# MAGIC
# MAGIC ```
# MAGIC NARROW (no shuffle):
# MAGIC Partition 1 → Transform → Partition 1
# MAGIC Partition 2 → Transform → Partition 2
# MAGIC Partition 3 → Transform → Partition 3
# MAGIC
# MAGIC WIDE (shuffle required):
# MAGIC Partition 1 ──┐
# MAGIC Partition 2 ──┼→ Shuffle → Partition 1
# MAGIC Partition 3 ──┼→ Shuffle → Partition 2
# MAGIC               └→ Shuffle → Partition 3
# MAGIC ```

# COMMAND ----------

print("=== Narrow Transformation Example ===\n")

# Narrow: filter, select, withColumn
df_narrow = df_employees \
    .filter(col("salary") > 80000) \
    .withColumn("salary_bonus", col("salary") * 1.1) \
    .select("name", "department", "salary_bonus")

print("Operations: filter → withColumn → select (all narrow)")
print("\nExplain plan:")
df_narrow.explain()

print("\n✅ Narrow transformation characteristics:")
print("  - No Exchange (shuffle) operations")
print("  - Single stage execution")
print("  - Data stays in same partition")
print("  - Very fast execution")

# COMMAND ----------

print("=== Wide Transformation Example ===\n")

# Wide: groupBy requires shuffle
df_wide = df_employees \
    .groupBy("department") \
    .agg({"salary": "avg"}) \
    .withColumnRenamed("avg(salary)", "avg_salary")

print("Operations: groupBy → agg (wide transformation)")
print("\nExplain plan:")
df_wide.explain()

print("\n✅ Wide transformation characteristics:")
print("  - Exchange (shuffle) operation present")
print("  - Multiple stages (before and after shuffle)")
print("  - Data moves across network")
print("  - More expensive than narrow")

print("\nResult:")
df_wide.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Partitioning and Shuffling Explained
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Partitioning**: How data is divided across executor nodes
# MAGIC
# MAGIC **Default Partitioning:**
# MAGIC - Input files → One partition per file (or file split)
# MAGIC - After shuffle → `spark.sql.shuffle.partitions` (default 200)
# MAGIC
# MAGIC **Shuffle**: Moving data between partitions
# MAGIC - Triggered by wide transformations
# MAGIC - Expensive: disk I/O + network transfer + serialization
# MAGIC - Creates stage boundaries
# MAGIC
# MAGIC **Optimization Goal**: Minimize shuffles, optimize partition count

# COMMAND ----------

print("=== Understanding Partitions ===\n")

# Create DataFrame with known number of partitions
df_partitioned = spark.createDataFrame(employees_data, schema).repartition(3)

print(f"Number of partitions: {df_partitioned.rdd.getNumPartitions()}")

# Show data distribution across partitions
def show_partition_distribution(df, name):
    print(f"\n{name}:")
    partition_counts = df.rdd.mapPartitionsWithIndex(
        lambda idx, it: [(idx, sum(1 for _ in it))]
    ).collect()
    for partition_id, count in partition_counts:
        print(f"  Partition {partition_id}: {count} records")

show_partition_distribution(df_partitioned, "Original distribution (3 partitions)")

# COMMAND ----------

print("=== Shuffle Impact ===\n")

# Operation WITHOUT shuffle (narrow)
df_no_shuffle = df_partitioned.filter(col("salary") > 80000)
print("After filter (narrow - no shuffle):")
show_partition_distribution(df_no_shuffle, "Filtered data")
print("  ✅ Partition count unchanged (3)")
print("  ✅ No data movement")

# Operation WITH shuffle (wide)
df_with_shuffle = df_partitioned.groupBy("department").count()
print("\n\nAfter groupBy (wide - with shuffle):")
print(f"  Partition count: {df_with_shuffle.rdd.getNumPartitions()}")
print("  ⚠️ Changed to spark.sql.shuffle.partitions (default 200)")

# Set shuffle partitions to reasonable number
spark.conf.set("spark.sql.shuffle.partitions", "3")
df_with_shuffle_optimized = df_partitioned.groupBy("department").count()
print(f"\n  After setting shuffle partitions to 3: {df_with_shuffle_optimized.rdd.getNumPartitions()}")

df_with_shuffle_optimized.show()

print("\n✅ Partition optimization:")
print("  - Too many partitions → overhead from task scheduling")
print("  - Too few partitions → underutilized cluster resources")
print("  - Sweet spot: 2-4x number of executor cores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Catalyst Optimizer Deep Dive
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Catalyst Optimizer**: Spark's query optimization engine
# MAGIC
# MAGIC **Optimization Phases:**
# MAGIC 1. **Analysis**: Resolve column names, types (unresolved → resolved logical plan)
# MAGIC 2. **Logical Optimization**: Rule-based optimizations
# MAGIC    - Predicate pushdown
# MAGIC    - Column pruning
# MAGIC    - Constant folding
# MAGIC    - Boolean simplification
# MAGIC 3. **Physical Planning**: Generate execution strategies
# MAGIC 4. **Code Generation**: Generate optimized bytecode
# MAGIC
# MAGIC ### Key Optimizations
# MAGIC
# MAGIC - **Predicate Pushdown**: Move filters as early as possible
# MAGIC - **Column Pruning**: Only read necessary columns
# MAGIC - **Constant Folding**: Evaluate constants at compile time
# MAGIC - **Join Reordering**: Optimize join order for efficiency

# COMMAND ----------

print("=== Catalyst Optimization Example 1: Predicate Pushdown ===\n")

# Create a query that could benefit from optimization
df_unoptimized = df_employees \
    .select("emp_id", "name", "department", "salary") \
    .withColumn("salary_doubled", col("salary") * 2) \
    .filter(col("department") == "Engineering") \
    .filter(col("salary") > 90000)

print("Original query order:")
print("  1. Select all columns")
print("  2. Add calculated column")
print("  3. Filter by department")
print("  4. Filter by salary")

print("\nCatalyst optimized plan:")
print("-" * 60)
df_unoptimized.explain(mode="formatted")

print("\n✅ Catalyst applied:")
print("  - Pushed filters before column selection")
print("  - Combined multiple filters")
print("  - Only reads necessary data")

# COMMAND ----------

print("=== Catalyst Optimization Example 2: Column Pruning ===\n")

# Query that only needs specific columns
df_pruned = df_employees \
    .withColumn("temp1", col("salary") * 1.1) \
    .withColumn("temp2", col("salary") * 1.2) \
    .select("name", "temp1")  # Only need name and temp1

print("Logical plan (without optimization):")
print("  - Compute temp1 (salary * 1.1)")
print("  - Compute temp2 (salary * 1.2) ← NOT NEEDED!")
print("  - Select name, temp1")

print("\nCatalyst optimized plan:")
print("-" * 60)
df_pruned.explain(mode="formatted")

print("\n✅ Catalyst optimization:")
print("  - Detected temp2 is unused")
print("  - Eliminated unnecessary computation")
print("  - Only computes what's needed for final result")

# COMMAND ----------

print("=== Catalyst Optimization Example 3: Constant Folding ===\n")

# Query with constants that can be pre-computed
df_constants = df_employees \
    .filter(col("salary") > (100 * 1000)) \
    .withColumn("tax_rate", lit(0.3)) \
    .withColumn("tax", col("salary") * 0.3)

print("Original query:")
print("  - Filter: salary > (100 * 1000) ← Constant expression")
print("  - Add tax_rate: 0.3 ← Constant")

print("\nCatalyst optimized plan:")
print("-" * 60)
df_constants.explain(mode="formatted")

print("\n✅ Constant folding:")
print("  - Pre-computed: 100 * 1000 = 100000")
print("  - Compiled into optimized bytecode")
print("  - No runtime calculation of constants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Caching and Persistence Strategies
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Caching**: Store computed DataFrame in memory/disk for reuse
# MAGIC
# MAGIC **When to Cache:**
# MAGIC - ✅ DataFrame used multiple times in different actions
# MAGIC - ✅ Expensive computation (joins, aggregations)
# MAGIC - ✅ Iterative algorithms (ML training)
# MAGIC
# MAGIC **When NOT to Cache:**
# MAGIC - ❌ DataFrame used only once
# MAGIC - ❌ Simple transformations (filter, select)
# MAGIC - ❌ Limited memory available
# MAGIC
# MAGIC **Storage Levels:**
# MAGIC - `MEMORY_ONLY`: Fast but limited by RAM
# MAGIC - `MEMORY_AND_DISK`: Spill to disk if needed
# MAGIC - `DISK_ONLY`: Slow but handles large data
# MAGIC - `MEMORY_ONLY_SER`: Serialized (less memory, slower)

# COMMAND ----------

from pyspark import StorageLevel
import time

print("=== Caching Impact Demonstration ===\n")

# Create a more expensive operation
large_data = [(i, f"Name_{i}", f"Dept_{i % 5}", i * 1000) for i in range(1, 10001)]
df_large = spark.createDataFrame(large_data, schema)

# Expensive transformation (multiple aggregations)
df_expensive = df_large \
    .groupBy("department") \
    .agg({"salary": "avg", "emp_id": "count"}) \
    .withColumnRenamed("avg(salary)", "avg_salary") \
    .withColumnRenamed("count(emp_id)", "emp_count")

print("Scenario: Using expensive DataFrame multiple times\n")

# WITHOUT caching
print("WITHOUT caching:")
start = time.time()
df_expensive.count()  # Action 1
df_expensive.show(5)  # Action 2
end_no_cache = time.time() - start
print(f"  Execution time: {end_no_cache:.3f} seconds")
print("  ⚠️ Recomputed aggregation twice!")

# COMMAND ----------

# WITH caching
print("\nWITH caching:")
df_expensive.cache()  # Mark for caching
start = time.time()
df_expensive.count()  # Action 1 - triggers caching
df_expensive.show(5)  # Action 2 - uses cache
end_with_cache = time.time() - start
print(f"  Execution time: {end_with_cache:.3f} seconds")
print("  ✅ Computed once, reused from cache!")

print(f"\nPerformance improvement: {(end_no_cache / end_with_cache):.2f}x faster")

# Check cache status
print("\nCache statistics:")
print(f"  Is cached: {df_expensive.is_cached}")
print(f"  Storage level: {df_expensive.storageLevel}")

# COMMAND ----------

print("=== Different Storage Levels ===\n")

# Clean up previous cache
df_expensive.unpersist()

# Compare storage levels
from pyspark import StorageLevel

print("Available storage levels:\n")

storage_levels = [
    ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY, "Fast, in-memory only"),
    ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK, "Spill to disk if needed"),
    ("DISK_ONLY", StorageLevel.DISK_ONLY, "All on disk"),
    ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER, "Serialized in memory")
]

for name, level, description in storage_levels:
    print(f"{name}:")
    print(f"  Description: {description}")
    print(f"  Use Memory: {level.useMemory}")
    print(f"  Use Disk: {level.useDisk}")
    print(f"  Deserialized: {level.deserialized}")
    print()

# COMMAND ----------

print("=== Caching Best Practices ===\n")

# Example: ML pipeline with iterative training
df_features = df_large \
    .filter(col("salary") > 50000) \
    .withColumn("salary_normalized", col("salary") / 100000)

print("Use case: Machine learning with multiple iterations")
print("\nBEST PRACTICE #1: Cache before iterative operations")

df_features.cache()
df_features.count()  # Materialize cache

print("  ✅ Cached before training loop")
print("  ✅ Each iteration uses cached data")

# Simulate multiple iterations
for i in range(3):
    result = df_features.agg({"salary_normalized": "avg"}).collect()
    print(f"  Iteration {i+1}: avg = {result[0][0]:.4f} (from cache)")

print("\nBEST PRACTICE #2: Unpersist when done")
df_features.unpersist()
print("  ✅ Freed memory for other operations")

# COMMAND ----------

print("\nBEST PRACTICE #3: Cache at the right level")

# Decision tree:
print("""
Decision tree for caching:
┌─ Used multiple times?
│  ├─ Yes ─→ Continue
│  └─ No ──→ Don't cache
│
├─ Expensive to compute?
│  ├─ Yes ─→ Continue
│  └─ No ──→ Don't cache
│
├─ Fits in memory?
│  ├─ Yes ─────→ MEMORY_ONLY
│  ├─ Partially → MEMORY_AND_DISK
│  └─ No ──────→ Consider DISK_ONLY or repartitioning
│
└─ Memory pressure?
   ├─ High ──→ MEMORY_ONLY_SER (serialized)
   └─ Low ───→ MEMORY_ONLY (deserialized, faster)
""")

print("\n✅ Remember:")
print("  - Caching is not free (serialization, memory)")
print("  - Cache only what you'll reuse")
print("  - Monitor with Spark UI")
print("  - Unpersist when done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### 1. Lazy Evaluation
# MAGIC
# MAGIC - **Transformations are lazy**: Build execution plan
# MAGIC - **Actions are eager**: Trigger computation
# MAGIC - **Benefits**: Optimization, efficiency, pipelining
# MAGIC - **Use `explain()`**: Understand execution plan
# MAGIC
# MAGIC ### 2. Narrow vs Wide Transformations
# MAGIC
# MAGIC - **Narrow**: One-to-one partition mapping, no shuffle
# MAGIC   - Examples: `filter`, `select`, `map`, `withColumn`
# MAGIC - **Wide**: Many-to-many partition mapping, requires shuffle
# MAGIC   - Examples: `groupBy`, `join`, `orderBy`, `distinct`
# MAGIC - **Performance**: Narrow is fast, wide is expensive
# MAGIC
# MAGIC ### 3. Partitioning and Shuffling
# MAGIC
# MAGIC - **Partitions**: Data distributed across executors
# MAGIC - **Shuffle**: Data movement between partitions (expensive!)
# MAGIC - **Optimization**:
# MAGIC   - Set `spark.sql.shuffle.partitions` appropriately
# MAGIC   - Aim for 2-4x executor cores
# MAGIC   - Use `repartition()` strategically
# MAGIC
# MAGIC ### 4. Catalyst Optimizer
# MAGIC
# MAGIC - **Predicate Pushdown**: Filter early
# MAGIC - **Column Pruning**: Read only needed columns
# MAGIC - **Constant Folding**: Pre-compute constants
# MAGIC - **Trust Catalyst**: It optimizes your queries!
# MAGIC
# MAGIC ### 5. Caching Strategies
# MAGIC
# MAGIC - **Cache when**:
# MAGIC   - Used multiple times
# MAGIC   - Expensive to compute
# MAGIC   - Iterative algorithms
# MAGIC - **Storage levels**:
# MAGIC   - `MEMORY_ONLY`: Fast, limited by RAM
# MAGIC   - `MEMORY_AND_DISK`: Safe default
# MAGIC   - `MEMORY_ONLY_SER`: Save memory, slower
# MAGIC - **Don't forget**: `unpersist()` when done!
# MAGIC
# MAGIC ### Performance Tuning Checklist
# MAGIC
# MAGIC - ✅ Understand your execution plan (`explain()`)
# MAGIC - ✅ Minimize wide transformations (shuffles)
# MAGIC - ✅ Optimize partition count
# MAGIC - ✅ Use explicit schemas (avoid inference)
# MAGIC - ✅ Cache strategically (not everything!)
# MAGIC - ✅ Leverage Catalyst (trust the optimizer)
# MAGIC - ✅ Monitor with Spark UI
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Practice with larger datasets
# MAGIC - Analyze Spark UI for bottlenecks
# MAGIC - Learn about join strategies (broadcast, sort-merge)
# MAGIC - Explore adaptive query execution (AQE)
# MAGIC - Study data skew handling techniques

# COMMAND ----------

# Clean up
spark.catalog.clearCache()
print("=== Cleanup Complete ===")
print("All caches cleared")