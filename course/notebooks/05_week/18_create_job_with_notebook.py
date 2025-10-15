# Databricks notebook source
# MAGIC %md
# MAGIC # Create Databricks Job with Notebook Task - Week 5
# MAGIC
# MAGIC Learn to create and configure Databricks jobs programmatically.

# COMMAND ----------

# Job configuration (would use Databricks Jobs API in production)
job_config = {
    "name": "Sales ETL Pipeline",
    "tasks": [
        {
            "task_key": "bronze_ingestion",
            "notebook_task": {
                "notebook_path": "/Workspace/course/notebooks/02_week/04_file_ingestion",
                "source": "WORKSPACE"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        },
        {
            "task_key": "silver_transformation",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": "/Workspace/course/notebooks/03_week/08_simple_transformations",
                "source": "WORKSPACE"
            }
        },
        {
            "task_key": "gold_aggregation",
            "depends_on": [{"task_key": "silver_transformation"}],
            "notebook_task": {
                "notebook_path": "/Workspace/course/notebooks/03_week/10_aggregations",
                "source": "WORKSPACE"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "America/New_York"
    }
}

print("=== Job Configuration ===")
print(f"Job Name: {job_config['name']}")
print(f"Tasks: {len(job_config['tasks'])}")
print(f"Schedule: {job_config['schedule']['quartz_cron_expression']}")
print("\nTask Dependencies:")
for task in job_config['tasks']:
    depends = task.get('depends_on', [])
    if depends:
        print(f"  {task['task_key']} depends on: {[d['task_key'] for d in depends]}")
    else:
        print(f"  {task['task_key']} (root task)")

# In production: use Databricks Jobs API to create job
# import requests
# response = requests.post(f"{databricks_host}/api/2.1/jobs/create", json=job_config)
