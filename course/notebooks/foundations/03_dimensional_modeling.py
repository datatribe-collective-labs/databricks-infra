# Databricks notebook source
# MAGIC %md
# MAGIC # Dimensional Modeling
# MAGIC 
# MAGIC ## What is Dimensional Modeling?
# MAGIC 
# MAGIC Dimensional modeling is a data modeling technique optimized for analytical queries. It organizes data into:
# MAGIC 
# MAGIC - **📊 Fact Tables**: Measurements, metrics, events (things that happened)
# MAGIC - **🏷️ Dimension Tables**: Context, attributes, descriptors (who, what, where, when, why)
# MAGIC 
# MAGIC This separation makes queries intuitive and fast.
# MAGIC 
# MAGIC ## Why Dimensional Modeling?
# MAGIC 
# MAGIC **Benefits:**
# MAGIC - Intuitive for business users ("Show me sales by product by region")
# MAGIC - Excellent query performance (fewer joins, better indexes)
# MAGIC - Flexible for ad-hoc analysis
# MAGIC - Handles historical data well
# MAGIC - Widely understood pattern (taught in every data warehouse course)
# MAGIC 
# MAGIC **When to use it:**
# MAGIC - Analytical/BI workloads (not transactional systems)
# MAGIC - Data warehouses and lakehouses
# MAGIC - Reporting and dashboards
# MAGIC - Business intelligence tools

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Tables vs Dimension Tables
# MAGIC 
# MAGIC ### 📊 Fact Tables
# MAGIC **Characteristics:**
# MAGIC - Store measurements, metrics, events
# MAGIC - Usually numeric values (amounts, quantities, durations)
# MAGIC - Many rows (millions to billions)
# MAGIC - Foreign keys to dimension tables
# MAGIC - Represent business processes or events
# MAGIC 
# MAGIC **Examples:**
# MAGIC - `fact_meter_readings`: Energy consumption measurements
# MAGIC - `fact_sales`: Transaction amounts
# MAGIC - `fact_website_clicks`: User interaction events
# MAGIC 
# MAGIC ### 🏷️ Dimension Tables
# MAGIC **Characteristics:**
# MAGIC - Store descriptive attributes
# MAGIC - Usually text/categorical data
# MAGIC - Fewer rows (thousands to millions)
# MAGIC - Provide context to facts
# MAGIC - Answer: who, what, where, when, why
# MAGIC 
# MAGIC **Examples:**
# MAGIC - `dim_customer`: Customer details (name, type, location)
# MAGIC - `dim_product`: Product attributes (name, category, price)
# MAGIC - `dim_date`: Calendar attributes (day, month, quarter, holiday)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Energy Consumption Model
# MAGIC 
# MAGIC Let's build a dimensional model for smart meter data.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType, BooleanType
from pyspark.sql.functions import col

# Fact Table: Meter Readings
fact_readings_data = [
    (1, "MTR001", "CUST_A", "2023-11-14 00:00:00", 125.5, 22.5, "H"),
    (2, "MTR002", "CUST_B", "2023-11-14 00:00:00", 89.3, 18.2, "H"),
    (3, "MTR003", "CUST_C", "2023-11-14 00:00:00", 210.4, 42.1, "H"),
    (4, "MTR001", "CUST_A", "2023-11-14 01:00:00", 126.8, 23.1, "H"),
    (5, "MTR002", "CUST_B", "2023-11-14 01:00:00", 91.5, 19.0, "H"),
    (6, "MTR003", "CUST_C", "2023-11-14 01:00:00", 215.2, 43.8, "H"),
    (7, "MTR001", "CUST_A", "2023-11-14 02:00:00", 128.1, 23.7, "H"),
    (8, "MTR002", "CUST_B", "2023-11-14 02:00:00", 93.2, 19.5, "H"),
]

fact_schema = StructType([
    StructField("reading_id", IntegerType(), False),
    StructField("meter_id", StringType(), False),  # FK to dim_meter
    StructField("customer_id", StringType(), False),  # FK to dim_customer
    StructField("reading_timestamp", StringType(), False),  # FK to dim_date
    StructField("energy_consumption_kwh", DoubleType(), False),  # Metric
    StructField("cost_eur", DoubleType(), False),  # Metric
    StructField("reading_interval", StringType(), True),  # H=Hourly, D=Daily
])

fact_meter_readings = spark.createDataFrame(fact_readings_data, fact_schema)

print("📊 FACT TABLE: fact_meter_readings")
display(fact_meter_readings)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Table Observations
# MAGIC 
# MAGIC **What makes this a fact table:**
# MAGIC - ✅ Contains measurements: `energy_consumption_kwh`, `cost_eur`
# MAGIC - ✅ Has foreign keys: `meter_id`, `customer_id`
# MAGIC - ✅ Represents events: each row is a meter reading at a point in time
# MAGIC - ✅ Grain is clear: One row = one reading from one meter at one timestamp
# MAGIC - ✅ Many rows (grows continuously as meters report)
# MAGIC 
# MAGIC **Note:** In production, fact tables can have millions or billions of rows!

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's create the dimension tables that provide context:

# COMMAND ----------

# Dimension Table: Customers
dim_customer_data = [
    ("CUST_A", "ABC Manufacturing", "Industrial", "Helsinki", "Finland", "2020-01-15", True, 5000),
    ("CUST_B", "Tech Startup Oy", "Commercial", "Espoo", "Finland", "2021-06-20", True, 150),
    ("CUST_C", "Residential User", "Residential", "Vantaa", "Finland", "2022-03-10", True, 0),
]

dim_customer_schema = StructType([
    StructField("customer_id", StringType(), False),  # Primary Key
    StructField("customer_name", StringType(), False),
    StructField("customer_type", StringType(), False),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_since", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("avg_monthly_consumption_kwh", IntegerType(), True),
])

dim_customer = spark.createDataFrame(dim_customer_data, dim_customer_schema)

print("🏷️ DIMENSION TABLE: dim_customer")
display(dim_customer)

# COMMAND ----------

# Dimension Table: Meters
dim_meter_data = [
    ("MTR001", "Smart Meter Pro v2.1", "Smart", "CUST_A", "2020-02-01", True, "Helsinki Grid A"),
    ("MTR002", "Smart Meter Pro v2.1", "Smart", "CUST_B", "2021-07-01", True, "Espoo Grid B"),
    ("MTR003", "Smart Meter Pro v3.0", "Smart", "CUST_C", "2022-04-01", True, "Vantaa Grid C"),
]

dim_meter_schema = StructType([
    StructField("meter_id", StringType(), False),  # Primary Key
    StructField("meter_model", StringType(), True),
    StructField("meter_type", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("installation_date", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("grid_location", StringType(), True),
])

dim_meter = spark.createDataFrame(dim_meter_data, dim_meter_schema)

print("🏷️ DIMENSION TABLE: dim_meter")
display(dim_meter)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Table Observations
# MAGIC 
# MAGIC **What makes these dimension tables:**
# MAGIC - ✅ Contain descriptive attributes (names, types, locations)
# MAGIC - ✅ Relatively few rows (3 customers, 3 meters)
# MAGIC - ✅ Provide context for fact table measurements
# MAGIC - ✅ Can be joined to facts via foreign keys
# MAGIC - ✅ Mostly text/categorical data
# MAGIC 
# MAGIC **Key principle:** Dimensions answer "who, what, where, when, why"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star Schema: Joining Facts and Dimensions
# MAGIC 
# MAGIC A **star schema** has:
# MAGIC - One central fact table
# MAGIC - Multiple dimension tables connected to it
# MAGIC - Looks like a star ⭐ when diagrammed
# MAGIC 
# MAGIC ```
# MAGIC        dim_customer
# MAGIC              |
# MAGIC              |
# MAGIC  dim_meter---fact_meter_readings---dim_date
# MAGIC              |
# MAGIC              |
# MAGIC        dim_location
# MAGIC ```
# MAGIC 
# MAGIC Let's query this star schema:

# COMMAND ----------

# Query 1: Total consumption by customer type
result = fact_meter_readings \
    .join(dim_customer, "customer_id") \
    .groupBy("customer_type") \
    .agg({"energy_consumption_kwh": "sum", "cost_eur": "sum"}) \
    .withColumnRenamed("sum(energy_consumption_kwh)", "total_consumption_kwh") \
    .withColumnRenamed("sum(cost_eur)", "total_cost_eur") \
    .orderBy("total_consumption_kwh", ascending=False)

print("📈 Query: Total Consumption by Customer Type")
display(result)

# COMMAND ----------

# Query 2: Average consumption by meter model
result = fact_meter_readings \
    .join(dim_meter, "meter_id") \
    .groupBy("meter_model") \
    .agg({"energy_consumption_kwh": "avg"}) \
    .withColumnRenamed("avg(energy_consumption_kwh)", "avg_consumption_kwh") \
    .orderBy("avg_consumption_kwh", ascending=False)

print("📈 Query: Average Consumption by Meter Model")
display(result)

# COMMAND ----------

# Query 3: Customer details with consumption
result = fact_meter_readings \
    .join(dim_customer, "customer_id") \
    .join(dim_meter, "meter_id") \
    .groupBy(
        "customer_name", 
        "customer_type", 
        "city", 
        "meter_model"
    ) \
    .agg({
        "energy_consumption_kwh": "sum",
        "reading_id": "count"
    }) \
    .withColumnRenamed("sum(energy_consumption_kwh)", "total_consumption_kwh") \
    .withColumnRenamed("count(reading_id)", "num_readings") \
    .orderBy("total_consumption_kwh", ascending=False)

print("📈 Query: Customer Consumption Summary")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Star Schema Works Well
# MAGIC 
# MAGIC ✅ **Simple joins**: Each dimension joins directly to fact
# MAGIC ✅ **Performance**: Fewer joins needed, BI tools optimize well
# MAGIC ✅ **Intuitive**: Business users understand the model easily
# MAGIC ✅ **Flexible**: Easy to add new dimensions without changing existing queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grain: The Most Important Decision
# MAGIC 
# MAGIC **Grain** = What does one row in the fact table represent?
# MAGIC 
# MAGIC ### Our Example Grain:
# MAGIC "One reading from one meter at one point in time"
# MAGIC 
# MAGIC ### Why Grain Matters:
# MAGIC 
# MAGIC ✅ **Correct grain:**
# MAGIC ```
# MAGIC reading_id | meter_id | timestamp           | consumption_kwh
# MAGIC 1          | MTR001   | 2023-11-14 00:00:00 | 125.5
# MAGIC 2          | MTR001   | 2023-11-14 01:00:00 | 126.8
# MAGIC ```
# MAGIC - Clear: each row is one measurement
# MAGIC - Can aggregate to any time period
# MAGIC - No double-counting issues
# MAGIC 
# MAGIC ❌ **Mixed grain (BAD):**
# MAGIC ```
# MAGIC reading_id | meter_id | timestamp           | consumption_kwh
# MAGIC 1          | MTR001   | 2023-11-14 00:00:00 | 125.5        # Hourly
# MAGIC 2          | MTR001   | 2023-11-14          | 3024.5       # Daily total - WRONG!
# MAGIC ```
# MAGIC - Confusing: some rows hourly, some daily
# MAGIC - Aggregations will double-count
# MAGIC - Queries need complex filters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table Types
# MAGIC 
# MAGIC ### 1. Transaction Facts (Most Common)
# MAGIC - One row per business event
# MAGIC - Examples: sales, meter readings, website clicks
# MAGIC - Our `fact_meter_readings` is a transaction fact
# MAGIC 
# MAGIC ```python
# MAGIC # Example grain: One row per sale
# MAGIC fact_sales: sale_id, product_id, customer_id, date, quantity, amount
# MAGIC ```
# MAGIC 
# MAGIC ### 2. Periodic Snapshot Facts
# MAGIC - One row per time period
# MAGIC - Examples: daily inventory levels, monthly account balances
# MAGIC 
# MAGIC ```python
# MAGIC # Example grain: One row per product per day
# MAGIC fact_inventory_daily: product_id, date, quantity_on_hand, quantity_reserved
# MAGIC ```
# MAGIC 
# MAGIC ### 3. Accumulating Snapshot Facts
# MAGIC - One row per process/lifecycle
# MAGIC - Updates as process progresses
# MAGIC - Examples: order fulfillment, loan processing
# MAGIC 
# MAGIC ```python
# MAGIC # Example grain: One row per order
# MAGIC fact_order_lifecycle: order_id, order_date, ship_date, delivery_date, days_to_ship
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additive vs Non-Additive Facts
# MAGIC 
# MAGIC ### Additive Facts (Preferred)
# MAGIC Can be summed across all dimensions
# MAGIC ```python
# MAGIC # These make sense to sum:
# MAGIC energy_consumption_kwh  # Sum across time, customers, meters ✅
# MAGIC sales_amount_eur        # Sum across products, stores, dates ✅
# MAGIC quantity_sold           # Sum across all dimensions ✅
# MAGIC ```
# MAGIC 
# MAGIC ### Semi-Additive Facts
# MAGIC Can be summed across some dimensions, not others
# MAGIC ```python
# MAGIC # Account balance
# MAGIC account_balance  # Can sum across accounts, NOT across time ⚠️
# MAGIC                  # (summing Monday's balance + Tuesday's balance = nonsense)
# MAGIC ```
# MAGIC 
# MAGIC ### Non-Additive Facts
# MAGIC Cannot be summed - use averages or other aggregations
# MAGIC ```python
# MAGIC # These don't make sense to sum:
# MAGIC temperature_celsius     # Use AVG ❌
# MAGIC percentage_value        # Use AVG ❌
# MAGIC unit_price             # Use AVG or weighted avg ❌
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Denormalization in Dimensional Models
# MAGIC 
# MAGIC Unlike transactional databases (OLTP), dimensional models embrace denormalization.
# MAGIC 
# MAGIC ### Example: Should we split customer address?
# MAGIC 
# MAGIC **Normalized (OLTP style):**
# MAGIC ```
# MAGIC dim_customer: customer_id, name, address_id
# MAGIC dim_address: address_id, street, city, country
# MAGIC ```
# MAGIC - More tables to join
# MAGIC - Harder queries
# MAGIC - Minimal storage savings in modern systems
# MAGIC 
# MAGIC **Denormalized (Dimensional style):**
# MAGIC ```
# MAGIC dim_customer: customer_id, name, street, city, country
# MAGIC ```
# MAGIC - Simpler queries
# MAGIC - Better performance
# MAGIC - Slight data duplication OK
# MAGIC 
# MAGIC ### When to Denormalize:
# MAGIC ✅ Dimensions: Almost always
# MAGIC ⚠️ Facts: Be careful - can explode table size
# MAGIC 
# MAGIC ### Rule of thumb:
# MAGIC "If two attributes change together, keep them together"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Snowflake Schema (Not Recommended)
# MAGIC 
# MAGIC A **snowflake schema** normalizes dimensions into sub-dimensions.
# MAGIC 
# MAGIC ```
# MAGIC        dim_customer
# MAGIC              |
# MAGIC        dim_customer_type---fact_sales---dim_product
# MAGIC                                              |
# MAGIC                                         dim_category
# MAGIC ```
# MAGIC 
# MAGIC ### Why avoid snowflaking?
# MAGIC - ❌ More joins = slower queries
# MAGIC - ❌ More complex for business users
# MAGIC - ❌ Minimal storage benefit in cloud warehouses
# MAGIC - ❌ BI tools perform worse
# MAGIC 
# MAGIC ### When snowflaking makes sense:
# MAGIC - ✅ Dimension has 100+ attributes
# MAGIC - ✅ Clear, frequently used sub-groupings
# MAGIC - ✅ Updates to sub-dimensions are frequent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Degenerate Dimensions
# MAGIC 
# MAGIC Sometimes "dimensions" live in the fact table itself.
# MAGIC 
# MAGIC ### Example:

# COMMAND ----------

# Fact table with degenerate dimension
fact_with_degenerate = [
    (1, "MTR001", "CUST_A", "2023-11-14 00:00:00", 125.5, "H", "INV-001"),  # invoice_number
    (2, "MTR002", "CUST_B", "2023-11-14 00:00:00", 89.3, "H", "INV-002"),
    (3, "MTR003", "CUST_C", "2023-11-14 00:00:00", 210.4, "H", "INV-003"),
]

schema_deg = StructType([
    StructField("reading_id", IntegerType(), False),
    StructField("meter_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("reading_timestamp", StringType(), False),
    StructField("energy_consumption_kwh", DoubleType(), False),
    StructField("reading_interval", StringType(), True),
    StructField("invoice_number", StringType(), True),  # Degenerate dimension
])

fact_deg_df = spark.createDataFrame(fact_with_degenerate, schema_deg)

print("📊 Fact with Degenerate Dimension (invoice_number)")
display(fact_deg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What's a Degenerate Dimension?
# MAGIC 
# MAGIC - ✅ Acts like a dimension (you might filter or group by it)
# MAGIC - ✅ But has no other attributes worth a separate table
# MAGIC - ✅ Examples: invoice number, order number, transaction ID
# MAGIC 
# MAGIC **Why not make a separate dim_invoice table?**
# MAGIC - It would only have `invoice_number` (no other attributes)
# MAGIC - Wasteful to create a table with one column
# MAGIC - Just keep it in the fact table!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensional Modeling Best Practices
# MAGIC 
# MAGIC ### 1. Define Clear Grain
# MAGIC ```python
# MAGIC # Good: "One row per customer per day"
# MAGIC # Bad: "Customer data" (what grain?)
# MAGIC ```
# MAGIC 
# MAGIC ### 2. Use Business Terms
# MAGIC ```python
# MAGIC # Good: customer_name, total_sales_eur
# MAGIC # Bad: cust_nm, tot_sls_amt_val
# MAGIC ```
# MAGIC 
# MAGIC ### 3. Prefer Star Over Snowflake
# MAGIC ```python
# MAGIC # Keep dimensions denormalized
# MAGIC dim_customer: customer_id, name, type, city, country
# MAGIC 
# MAGIC # Avoid over-normalization
# MAGIC # dim_customer → dim_city → dim_country (too much!)
# MAGIC ```
# MAGIC 
# MAGIC ### 4. Use Surrogate Keys
# MAGIC ```python
# MAGIC # Use generated numeric keys as primary keys
# MAGIC customer_key (INT)  # Surrogate key
# MAGIC customer_id (STRING) # Natural/business key
# MAGIC 
# MAGIC # Benefits: faster joins, handles source system changes
# MAGIC ```
# MAGIC 
# MAGIC ### 5. Add Audit Columns
# MAGIC ```python
# MAGIC # Every table should have:
# MAGIC created_at, updated_at, created_by
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common Dimensional Modeling Mistakes
# MAGIC 
# MAGIC ### ❌ Mixing Facts and Dimensions
# MAGIC ```python
# MAGIC # BAD: Putting customer attributes in fact table
# MAGIC fact_sales: sale_id, product_id, customer_id, customer_name, customer_city, amount
# MAGIC 
# MAGIC # GOOD: Keep facts and dimensions separate
# MAGIC fact_sales: sale_id, product_id, customer_id, amount
# MAGIC dim_customer: customer_id, customer_name, customer_city
# MAGIC ```
# MAGIC 
# MAGIC ### ❌ Unclear Grain
# MAGIC ```python
# MAGIC # BAD: Mixing daily and monthly rows in same table
# MAGIC # GOOD: Separate tables with clear grain
# MAGIC fact_sales_daily   # One row per product per day
# MAGIC fact_sales_monthly # One row per product per month
# MAGIC ```
# MAGIC 
# MAGIC ### ❌ Too Much Normalization
# MAGIC ```python
# MAGIC # BAD: Over-snowflaking
# MAGIC dim_product → dim_category → dim_department → dim_division
# MAGIC 
# MAGIC # GOOD: Denormalize
# MAGIC dim_product: product_id, name, category, department, division
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's Next?
# MAGIC 
# MAGIC Now that you understand facts and dimensions, we'll explore:
# MAGIC 
# MAGIC **Slowly Changing Dimensions (SCDs)** - How to handle dimension attributes that change over time using Delta Lake features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Key Takeaways
# MAGIC 
# MAGIC - Dimensional modeling separates facts (measurements) from dimensions (context)
# MAGIC - Star schema = fact table surrounded by dimension tables
# MAGIC - **Grain** is the most critical decision: what does one row represent?
# MAGIC - Facts should be additive when possible
# MAGIC - Dimensions should be denormalized for performance
# MAGIC - Use business-friendly names and clear documentation
# MAGIC - Prefer star schema over snowflake schema
# MAGIC - Keep it simple: don't over-engineer!
