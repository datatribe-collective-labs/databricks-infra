# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to Data Modeling
# MAGIC 
# MAGIC ## What is Data Modeling?
# MAGIC 
# MAGIC Data modeling is the process of organizing and structuring data to make it:
# MAGIC - **Consistent**: Same concepts represented the same way across systems
# MAGIC - **Accessible**: Easy to query and understand for business users
# MAGIC - **Performant**: Optimized for the queries you need to run
# MAGIC - **Maintainable**: Clear structure that evolves with business needs
# MAGIC 
# MAGIC Think of it as creating a blueprint for how data should be stored and related to each other.
# MAGIC 
# MAGIC ## Why Does Data Modeling Matter?
# MAGIC 
# MAGIC Without proper data modeling:
# MAGIC - Queries become complex and slow
# MAGIC - Same metrics calculated differently across teams
# MAGIC - Data quality issues are harder to detect
# MAGIC - New team members struggle to understand the data
# MAGIC 
# MAGIC With good data modeling:
# MAGIC - Questions can be answered quickly with simple queries
# MAGIC - Single source of truth for business metrics
# MAGIC - Data quality rules are clear and enforceable
# MAGIC - Self-service analytics becomes possible

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: The Chaos of Unmodeled Data
# MAGIC 
# MAGIC Let's look at raw sensor data from smart meters as it arrives from IoT devices:

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import json

# Sample raw data as it comes from IoT devices (messy, inconsistent)
raw_data = [
    ('{"device_id": "MTR001", "ts": 1699920000, "reading": "125.5", "unit": "kWh", "customer": "CUST_A"}',),
    ('{"device_id": "MTR002", "timestamp": 1699920000, "value": 89.3, "customer_id": "CUST_B"}',),
    ('{"meter": "MTR001", "ts": 1699923600, "reading": "126.8", "unit": "kWh", "cust": "CUST_A"}',),
    ('{"device_id": "MTR003", "ts": 1699920000, "kwh": 210.4, "customer": "CUST_C"}',),
]

# Create DataFrame from raw JSON strings
raw_df = spark.createDataFrame(raw_data, ["raw_json"])

display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Problems with Raw Data:
# MAGIC 
# MAGIC 1. **Inconsistent field names**: `ts`, `timestamp`, `reading`, `value`, `kwh`
# MAGIC 2. **Mixed data types**: strings vs numbers for readings
# MAGIC 3. **Different customer field names**: `customer`, `customer_id`, `cust`
# MAGIC 4. **Missing standard fields**: some records lack units
# MAGIC 
# MAGIC **Querying this data is painful!** You'd need complex CASE statements and type casting everywhere.

# COMMAND ----------

# MAGIC %md
# MAGIC ## After Data Modeling: Clean, Consistent Structure

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, from_unixtime

# Sample modeled data - consistent structure
modeled_data = [
    ("MTR001", "2023-11-14 00:00:00", 125.5, "kWh", "CUST_A"),
    ("MTR002", "2023-11-14 00:00:00", 89.3, "kWh", "CUST_B"),
    ("MTR001", "2023-11-14 01:00:00", 126.8, "kWh", "CUST_A"),
    ("MTR003", "2023-11-14 00:00:00", 210.4, "kWh", "CUST_C"),
]

schema = StructType([
    StructField("meter_id", StringType(), False),
    StructField("reading_timestamp", StringType(), False),
    StructField("energy_consumption_kwh", DoubleType(), False),
    StructField("unit", StringType(), False),
    StructField("customer_id", StringType(), False)
])

modeled_df = spark.createDataFrame(modeled_data, schema)

display(modeled_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benefits of Modeled Data:
# MAGIC 
# MAGIC 1. **Standard field names**: Everyone knows `meter_id` and `reading_timestamp`
# MAGIC 2. **Consistent types**: All readings are `DOUBLE`, timestamps are proper `TIMESTAMP`
# MAGIC 3. **Clear semantics**: `energy_consumption_kwh` tells you exactly what it is
# MAGIC 4. **Easy queries**: Simple SELECT statements work
# MAGIC 
# MAGIC Let's see the difference in querying:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Comparison

# COMMAND ----------

# Query on modeled data - simple and clear!
result = modeled_df.groupBy("customer_id") \
    .agg({"energy_consumption_kwh": "sum"}) \
    .withColumnRenamed("sum(energy_consumption_kwh)", "total_consumption_kwh")

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare this to what you'd need for raw data:
# MAGIC ```python
# MAGIC # Hypothetical query on raw data (don't run - just illustration)
# MAGIC raw_df.selectExpr(
# MAGIC     "get_json_object(raw_json, '$.customer') as customer1",
# MAGIC     "get_json_object(raw_json, '$.customer_id') as customer2",
# MAGIC     "get_json_object(raw_json, '$.cust') as customer3",
# MAGIC     "CAST(get_json_object(raw_json, '$.reading') as DOUBLE) as reading1",
# MAGIC     "CAST(get_json_object(raw_json, '$.value') as DOUBLE) as reading2",
# MAGIC     "CAST(get_json_object(raw_json, '$.kwh') as DOUBLE) as reading3"
# MAGIC ).selectExpr(
# MAGIC     "COALESCE(customer1, customer2, customer3) as customer_id",
# MAGIC     "COALESCE(reading1, reading2, reading3) as consumption"
# MAGIC ).groupBy("customer_id").sum("consumption")
# MAGIC ```
# MAGIC 
# MAGIC **This is a nightmare to maintain!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Principles of Data Modeling
# MAGIC 
# MAGIC ### 1. **Define the Grain**
# MAGIC What does one row represent?
# MAGIC - Our example: One meter reading at a specific point in time
# MAGIC - Each row should represent exactly one "thing" at the right level of detail
# MAGIC 
# MAGIC ### 2. **Consistency Over Flexibility**
# MAGIC - Same entity type → Same column names everywhere
# MAGIC - Same data types for same concepts
# MAGIC - Standard naming conventions (snake_case, descriptive names)
# MAGIC 
# MAGIC ### 3. **Business Alignment**
# MAGIC - Use terms business users understand
# MAGIC - `customer_id` not `cust_fk_id_v2`
# MAGIC - Model should match how people think about the domain
# MAGIC 
# MAGIC ### 4. **Think About Queries**
# MAGIC - What questions will be asked?
# MAGIC - How will tables be joined?
# MAGIC - What aggregations are common?
# MAGIC 
# MAGIC ### 5. **Document Everything**
# MAGIC - What does each field mean?
# MAGIC - Where does the data come from?
# MAGIC - What's the refresh frequency?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Anti-Patterns to Avoid
# MAGIC 
# MAGIC ### ❌ The "Everything in One Table" Approach
# MAGIC ```
# MAGIC customer_id | customer_name | reading_timestamp | consumption | meter_id | meter_type | meter_install_date
# MAGIC ```
# MAGIC - Customer info repeated for every reading
# MAGIC - Hard to update customer details
# MAGIC - Wastes storage
# MAGIC 
# MAGIC ### ❌ Generic Column Names
# MAGIC ```
# MAGIC field1, field2, field3, value1, value2
# MAGIC ```
# MAGIC - Nobody knows what they mean
# MAGIC - Requires tribal knowledge
# MAGIC 
# MAGIC ### ❌ No Clear Grain
# MAGIC ```
# MAGIC Some rows: hourly readings
# MAGIC Other rows: daily summaries
# MAGIC Mixed in the same table!
# MAGIC ```
# MAGIC - Aggregations become impossible
# MAGIC - Double-counting issues
# MAGIC 
# MAGIC ### ❌ Putting Everything in Strings
# MAGIC ```
# MAGIC "2023-11-14" as string
# MAGIC "125.5" as string
# MAGIC "true" as string
# MAGIC ```
# MAGIC - Loses type safety
# MAGIC - Poor query performance
# MAGIC - Comparison issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC 
# MAGIC In the following notebooks, we'll explore:
# MAGIC 
# MAGIC 1. **Medallion Architecture** - How to structure data layers (Bronze/Silver/Gold)
# MAGIC 2. **Dimensional Modeling** - Facts and dimensions for analytics
# MAGIC 3. **SCDs and Delta Patterns** - Handling changing data over time
# MAGIC 
# MAGIC Each concept builds on this foundation of **consistent, well-structured data**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Key Takeaways
# MAGIC 
# MAGIC - Data modeling is about creating structure and consistency
# MAGIC - Well-modeled data is easier to query, faster, and more reliable
# MAGIC - Start with clear grain definition and business alignment
# MAGIC - Avoid anti-patterns like generic names and mixed grains
# MAGIC - Good modeling saves hours of complex query writing later
