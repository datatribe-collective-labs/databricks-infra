# User Schema Management Guide

## Overview

The `src.user_schema` module provides automatic user-specific schema management for Databricks course notebooks, ensuring data isolation and preventing conflicts in multi-user environments.

## Problem Statement

**Before**: Multiple users writing to shared schemas caused data conflicts:
```
databricks_course/
├── shared_bronze/      ← ALL users write here (conflicts!)
├── shared_silver/      ← Data gets overwritten
└── shared_gold/        ← Unpredictable state
```

**After**: Each user has their own isolated workspace:
```
databricks_course/
├── chanukya_pekala/    ← chanukya.pekala@gmail.com's workspace
│   ├── bronze_sales_transactions
│   ├── silver_sales_cleaned
│   └── gold_daily_summary
├── komal_azram/        ← komal.azram@datatribe.group's workspace
│   └── bronze_sales_transactions
└── joonas_lalli/       ← joonas.lalli@datatribe.group's workspace
    └── bronze_customer_events
```

## Architecture

### Reference Catalogs (READ-ONLY)
```python
# Users READ from these reference catalogs
df_source = spark.table("sales_dev.bronze.sales_transactions")
df_marketing = spark.table("marketing_dev.bronze.campaigns")
```

###User Workspaces (READ + WRITE)
```python
# Users WRITE to their own schema
output_table = config.get_table_path("bronze", "sales_transactions")
# Result: databricks_course.chanukya_pekala.bronze_sales_transactions

df.write.saveAsTable(output_table)
```

## Usage in Notebooks

### Step 1: Import the Utility

```python
# COMMAND ----------
from src.user_schema import get_user_schema_config

# Initialize configuration (automatically gets user email from Databricks context)
config = get_user_schema_config(spark, dbutils)
config.print_config()  # Optional: Display configuration details
```

**Output Example:**
```
============================================================
User Schema Configuration
============================================================
User Email:       chanukya.pekala@gmail.com
User Schema:      chanukya_pekala
Catalog:          databricks_course
Full Schema Path: databricks_course.chanukya_pekala
============================================================
Example table paths:
  Bronze: databricks_course.chanukya_pekala.bronze_example_table
  Silver: databricks_course.chanukya_pekala.silver_example_table
  Gold:   databricks_course.chanukya_pekala.gold_example_table
============================================================
```

### Step 2: Build Table Paths

```python
# COMMAND ----------
# Build table paths using the utility
bronze_table = config.get_table_path("bronze", "sales_transactions")
silver_table = config.get_table_path("silver", "sales_cleaned")
gold_table = config.get_table_path("gold", "daily_summary")

print(f"Bronze: {bronze_table}")
print(f"Silver: {silver_table}")
print(f"Gold: {gold_table}")
```

**Output:**
```
Bronze: databricks_course.chanukya_pekala.bronze_sales_transactions
Silver: databricks_course.chanukya_pekala.silver_sales_cleaned
Gold: databricks_course.chanukya_pekala.gold_daily_summary
```

### Step 3: Use in Data Pipeline

```python
# COMMAND ----------
from pyspark.sql.functions import col, upper

# 1. READ from reference catalog (shared, read-only)
df_source = spark.table("sales_dev.bronze.sales_transactions")

# 2. Transform the data
df_cleaned = df_source \
    .filter(col("quantity") > 0) \
    .withColumn("location", upper(col("store_location")))

# 3. WRITE to user's personal schema (isolated)
silver_table = config.get_table_path("silver", "sales_cleaned")
df_cleaned.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"✅ Written to: {silver_table}")
```

## Complete Example

### Bronze → Silver → Gold Pipeline

```python
# COMMAND ----------
# Setup
from src.user_schema import get_user_schema_config
from pyspark.sql.functions import *

config = get_user_schema_config(spark, dbutils)

# COMMAND ----------
# Bronze Layer: Ingest raw data
from pyspark.sql.types import *
from datetime import date

schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("date", DateType(), False)
])

raw_data = [
    (1, 101, 1299.99, date(2024, 1, 15)),
    (2, 102, 59.98, date(2024, 1, 15)),
    (3, 103, 89.99, date(2024, 1, 16))
]

df_raw = spark.createDataFrame(raw_data, schema)

bronze_table = config.get_table_path("bronze", "transactions")
df_raw.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"✅ Bronze: {bronze_table}")

# COMMAND ----------
# Silver Layer: Clean and validate
df_bronze = spark.table(bronze_table)

df_silver = df_bronze.filter(col("amount") > 0)

silver_table = config.get_table_path("silver", "transactions_cleaned")
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"✅ Silver: {silver_table}")

# COMMAND ----------
# Gold Layer: Aggregate for analytics
df_silver = spark.table(silver_table)

df_gold = df_silver.groupBy("date").agg(
    sum("amount").alias("total_revenue"),
    count("*").alias("transaction_count")
)

gold_table = config.get_table_path("gold", "daily_summary")
df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)

print(f"✅ Gold: {gold_table}")
```

## API Reference

### `UserSchemaConfig` Class

#### Constructor
```python
UserSchemaConfig(spark, dbutils, catalog="databricks_course")
```

**Parameters:**
- `spark`: SparkSession instance from Databricks notebook
- `dbutils`: DBUtils instance from Databricks notebook
- `catalog` (optional): Target Unity Catalog name (default: "databricks_course")

**Attributes:**
- `catalog` (str): Target catalog name
- `user_schema` (str): User's personal schema name (e.g., "chanukya_pekala")
- `user_email` (str): Original user email address

#### Methods

**`get_table_path(layer: str, table_name: str) -> str`**

Build fully qualified table path in user's schema.

```python
bronze_table = config.get_table_path("bronze", "sales_transactions")
# Returns: "databricks_course.chanukya_pekala.bronze_sales_transactions"
```

**Parameters:**
- `layer`: Data layer ("bronze", "silver", or "gold")
- `table_name`: Base table name

**Returns:** Fully qualified table path

**`get_schema_path() -> str`**

Get fully qualified schema path for the user.

```python
schema_path = config.get_schema_path()
# Returns: "databricks_course.chanukya_pekala"
```

**`print_config()`**

Print current configuration for debugging.

### Helper Function

**`get_user_schema_config(spark, dbutils, catalog="databricks_course") -> UserSchemaConfig`**

Convenience function to create `UserSchemaConfig` instance.

```python
from src.user_schema import get_user_schema_config

config = get_user_schema_config(spark, dbutils)
```

## Email to Schema Name Conversion

The utility automatically converts user email addresses to valid SQL schema names:

| User Email | Schema Name |
|-----------|-------------|
| `chanukya.pekala@gmail.com` | `chanukya_pekala` |
| `komal.azram@datatribe.group` | `komal_azram` |
| `test-user+123@example.com` | `test_user_123` |
| `first.last@company.co.uk` | `first_last` |

**Conversion Rules:**
1. Extract username part (before `@`)
2. Replace all non-alphanumeric characters with underscore (`_`)
3. Convert to lowercase

## Table Naming Convention

Tables use a **flat structure with layer prefixes** within each user's schema:

```
databricks_course.chanukya_pekala.bronze_sales_transactions
databricks_course.chanukya_pekala.bronze_customer_events
databricks_course.chanukya_pekala.bronze_product_inventory
databricks_course.chanukya_pekala.silver_sales_cleaned
databricks_course.chanukya_pekala.silver_customer_segments
databricks_course.chanukya_pekala.gold_daily_summary
databricks_course.chanukya_pekala.gold_monthly_kpis
```

**Benefits:**
- Simple permissions model (all tables in one schema)
- Easy to query: `SHOW TABLES IN databricks_course.chanukya_pekala`
- Matches existing Terraform infrastructure setup
- Clear data lineage with layer prefixes

## Permissions

### Automatic Schema Creation

The utility automatically creates the user's schema if it doesn't exist:

```python
# This happens automatically during config initialization
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{user_schema}")
```

**Required Permissions:**
- `USE_CATALOG` on `databricks_course`
- `CREATE_SCHEMA` on `databricks_course`

### User Access Patterns

**Platform Admins** (via `platform_admins` group):
- ALL_PRIVILEGES on all catalogs and schemas
- Can read/write any user's workspace
- Can manage infrastructure

**Platform Students** (via `platform_students` group):
- ALL_PRIVILEGES on their own schema
- SELECT + USE_SCHEMA on all other user schemas (peer learning)
- SELECT + USE_SCHEMA on reference catalogs (sales_dev, marketing_dev)

## Migration Guide

### Converting Existing Notebooks

**Before:**
```python
# Old hardcoded approach
import re
CATALOG = "databricks_course"
USER_SCHEMA_RAW = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
USER_SCHEMA = re.sub(r'[^a-zA-Z0-9_]', '_', USER_SCHEMA_RAW.split('@')[0])
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{USER_SCHEMA}")

bronze_table = f"{CATALOG}.{USER_SCHEMA}.bronze_sales_transactions"
```

**After:**
```python
# New utility-based approach
from src.user_schema import get_user_schema_config

config = get_user_schema_config(spark, dbutils)
bronze_table = config.get_table_path("bronze", "sales_transactions")
```

### Search and Replace Patterns

1. **Replace setup code:**
   - Remove: `import re` (if only used for schema name conversion)
   - Remove: `CATALOG = "databricks_course"` variable
   - Remove: `USER_SCHEMA_RAW` and `USER_SCHEMA` logic
   - Remove: `spark.sql(f"CREATE SCHEMA IF NOT EXISTS...")`
   - Add: Import statement and config initialization

2. **Replace table path construction:**
   - Find: `f"{CATALOG}.{USER_SCHEMA}.bronze_{table_name}"`
   - Replace: `config.get_table_path("bronze", "{table_name}")`

3. **Replace table path patterns:**
   ```python
   # Before
   f"{CATALOG}.{USER_SCHEMA}.{BRONZE_SCHEMA}sales_transactions"

   # After
   config.get_table_path("bronze", "sales_transactions")
   ```

## Troubleshooting

### Issue: "Failed to get user email from Databricks context"

**Cause:** Running outside Databricks notebook environment or missing `dbutils`.

**Solution:** Ensure you're running in a Databricks notebook and `dbutils` is available.

### Issue: "Failed to create schema... Ensure you have CREATE_SCHEMA privileges"

**Cause:** User lacks `CREATE_SCHEMA` permission on the catalog.

**Solution:** Contact workspace admin to grant permissions or check Terraform configuration.

### Issue: "Invalid layer 'Bronze'. Must be one of: bronze, silver, gold"

**Cause:** Layer parameter must be lowercase.

**Solution:** Use lowercase layer names:
```python
# ❌ Wrong
config.get_table_path("Bronze", "sales")

# ✅ Correct
config.get_table_path("bronze", "sales")
```

## Best Practices

1. **Initialize once per notebook:**
   ```python
   # At the top of your notebook
   config = get_user_schema_config(spark, dbutils)
   ```

2. **Use descriptive table names:**
   ```python
   # ✅ Good
   config.get_table_path("bronze", "sales_transactions_raw")
   config.get_table_path("silver", "sales_transactions_cleaned")

   # ❌ Avoid
   config.get_table_path("bronze", "data")
   config.get_table_path("silver", "temp")
   ```

3. **Follow medallion architecture:**
   - **Bronze**: Raw ingested data (minimal transformations)
   - **Silver**: Cleaned and validated data
   - **Gold**: Business-level aggregates and analytics

4. **Reference catalogs are read-only:**
   ```python
   # ✅ Correct pattern
   df = spark.table("sales_dev.bronze.sales")  # Read from reference
   df_transformed = df.filter(...)
   df_transformed.write.saveAsTable(config.get_table_path("bronze", "my_sales"))  # Write to user schema

   # ❌ Never do this
   df.write.saveAsTable("sales_dev.bronze.sales")  # Don't write to reference catalogs!
   ```

5. **Debug with print_config():**
   ```python
   config = get_user_schema_config(spark, dbutils)
   config.print_config()  # Shows current configuration
   ```

## Updated Notebooks

The following notebooks have been updated to use this utility:

- ✅ `02_week/06_file_ingestion.py`

## Notebooks To Update

The following notebooks still use the old pattern and should be migrated:

- 🔧 `02_week/07_api_ingest.py`
- 🔧 `02_week/08_database_ingest.py`
- 🔧 `02_week/09_s3_ingest.py`
- 🔧 `03_week/11_simple_transformations.py`
- 🔧 `03_week/12_window_transformations.py`
- 🔧 `03_week/13_aggregations.py`
- 🔧 `04_week/15_file_to_aggregation.py`
- 🔧 `04_week/16_api_to_aggregation.py`
- 🔧 `05_week/19_create_job_with_wheel.py`

## Support

For questions or issues:
1. Check this guide first
2. Review the example notebook: `02_week/06_file_ingestion.py`
3. Contact course instructors or platform admins
4. Submit issues to the infrastructure repository
