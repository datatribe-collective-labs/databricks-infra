# Databricks notebook source
# MAGIC %md
# MAGIC # Create Job with Python Wheel - Week 5
# MAGIC
# MAGIC Package Python code as wheel and use in Databricks jobs.

# COMMAND ----------

# Wheel package configuration
wheel_config = {
    "name": "sales_analytics",
    "version": "1.0.0",
    "modules": ["data_ingestion", "transformations", "aggregations"]
}

# Job configuration using wheel
job_with_wheel = {
    "name": "Sales Analytics (Wheel)",
    "tasks": [
        {
            "task_key": "run_analytics",
            "python_wheel_task": {
                "package_name": "sales_analytics",
                "entry_point": "main",
                "parameters": ["--catalog", "sales_dev", "--date", "2024-01-20"]
            },
            "libraries": [
                {"whl": "dbfs:/FileStore/wheels/sales_analytics-1.0.0-py3-none-any.whl"}
            ],
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        }
    ]
}

print("=== Python Wheel Job Configuration ===")
print(f"Package: {wheel_config['name']} v{wheel_config['version']}")
print(f"Modules: {wheel_config['modules']}")
print(f"\nJob Task: {job_with_wheel['tasks'][0]['task_key']}")
print(f"Entry Point: {job_with_wheel['tasks'][0]['python_wheel_task']['entry_point']}")
print(f"Parameters: {job_with_wheel['tasks'][0]['python_wheel_task']['parameters']}")

# Sample wheel module code
sample_code = """
# sales_analytics/main.py
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    catalog = sys.argv[1]  # --catalog sales_dev
    date = sys.argv[2]      # --date 2024-01-20

    df = spark.table(f"{catalog}.silver.sales_transactions")
    df_filtered = df.filter(f"transaction_date = '{date}'")
    
    # Perform analytics...
    print(f"Processed {df_filtered.count()} records for {date}")

if __name__ == "__main__":
    main()
"""

print("\n=== Sample Wheel Module Code ===")
print(sample_code)
