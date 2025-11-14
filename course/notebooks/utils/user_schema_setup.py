# Databricks notebook source
# MAGIC %md
# MAGIC # User Schema Setup Utility
# MAGIC
# MAGIC This notebook provides user-specific schema configuration for data isolation in multi-user environments.
# MAGIC
# MAGIC ## Usage in other notebooks:
# MAGIC ```python
# MAGIC # COMMAND ----------
# MAGIC %run ../utils/user_schema_setup
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Now use the config object
# MAGIC bronze_table = get_table_path("bronze", "sales_transactions")
# MAGIC print(f"Writing to: {bronze_table}")
# MAGIC ```
# MAGIC
# MAGIC ## What this provides:
# MAGIC - `USER_EMAIL`: Current user's email address
# MAGIC - `USER_SCHEMA`: User's schema name (e.g., "chanukya_pekala")
# MAGIC - `CATALOG`: Target catalog ("databricks_course")
# MAGIC - `get_table_path(layer, table_name)`: Helper function to build table paths
# MAGIC - `get_schema_path()`: Get user's full schema path
# MAGIC - `print_user_config()`: Display current configuration
# MAGIC - `VOLUME_PATH`: Users writable volume path

# COMMAND ----------

import re

# Configuration constants
CATALOG = "databricks_course"

# Get logged-in user's email
USER_EMAIL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Convert email to valid schema name
# Example: chanukya.pekala@gmail.com -> chanukya_pekala
USER_SCHEMA = re.sub(r'[^a-zA-Z0-9_]', '_', USER_EMAIL.split('@')[0]).lower()

# Create user's schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{USER_SCHEMA}")


# Hardcoded volume name, can be changed
VOLUME_NAME = "scratch"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{USER_SCHEMA}.{VOLUME_NAME}")

# The POSIX-style path for your Spark writes
VOLUME_PATH = f"/Volumes/{CATALOG}/{USER_SCHEMA}/{VOLUME_NAME}/"

# COMMAND ----------

# Helper functions available to notebooks that %run this

def get_table_path(layer, table_name):
    """
    Build fully qualified table path in user's schema.

    Args:
        layer: Data layer ("bronze", "silver", or "gold")
        table_name: Base table name

    Returns:
        Fully qualified table path

    Example:
        >>> get_table_path("bronze", "sales_transactions")
        'databricks_course.chanukya_pekala.bronze_sales_transactions'
    """
    layer = layer.lower()
    if layer not in ["bronze", "silver", "gold"]:
        raise ValueError(f"Invalid layer '{layer}'. Must be one of: bronze, silver, gold")

    return f"{CATALOG}.{USER_SCHEMA}.{layer}_{table_name}"


def get_schema_path():
    """
    Get fully qualified schema path for the user.

    Returns:
        Schema path (e.g., "databricks_course.chanukya_pekala")
    """
    return f"{CATALOG}.{USER_SCHEMA}"


def print_user_config():
    """Print current user schema configuration"""
    print("=" * 60)
    print("User Schema Configuration")
    print("=" * 60)
    print(f"User Email:       {USER_EMAIL}")
    print(f"User Schema:      {USER_SCHEMA}")
    print(f"Catalog:          {CATALOG}")
    print(f"Full Schema Path: {get_schema_path()}")
    print("=" * 60)
    print("\nExample table paths:")
    print(f"  Bronze: {get_table_path('bronze', 'example_table')}")
    print(f"  Silver: {get_table_path('silver', 'example_table')}")
    print(f"  Gold:   {get_table_path('gold', 'example_table')}")
    print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Summary
# MAGIC
# MAGIC After running this notebook with `%run`, the following variables and functions are available:
# MAGIC
# MAGIC ### Variables:
# MAGIC - `CATALOG` - Target catalog name
# MAGIC - `USER_EMAIL` - Current user's email
# MAGIC - `USER_SCHEMA` - User's schema name
# MAGIC
# MAGIC ### Functions:
# MAGIC - `get_table_path(layer, table_name)` - Build table paths
# MAGIC - `get_schema_path()` - Get schema path
# MAGIC - `print_user_config()` - Display configuration
