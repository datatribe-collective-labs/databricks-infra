# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrate Tasks in Job - Week 5
# MAGIC
# MAGIC Build complex DAG workflows with task dependencies.

# COMMAND ----------

# Multi-task job with DAG structure
dag_job = {
    "name": "Complete Sales Data Pipeline",
    "tasks": [
        # Bronze ingestion tasks (parallel)
        {
            "task_key": "ingest_sales",
            "notebook_task": {
                "notebook_path": "/course/notebooks/02_week/04_file_ingestion"
            }
        },
        {
            "task_key": "ingest_customers",
            "notebook_task": {
                "notebook_path": "/course/notebooks/02_week/05_api_ingest"
            }
        },
        {
            "task_key": "ingest_products",
            "notebook_task": {
                "notebook_path": "/course/notebooks/02_week/06_database_ingest"
            }
        },
        
        # Silver transformation (depends on all bronze)
        {
            "task_key": "transform_sales",
            "depends_on": [
                {"task_key": "ingest_sales"},
                {"task_key": "ingest_customers"},
                {"task_key": "ingest_products"}
            ],
            "notebook_task": {
                "notebook_path": "/course/notebooks/03_week/08_simple_transformations"
            }
        },
        
        # Gold aggregations (parallel, depends on silver)
        {
            "task_key": "daily_aggregation",
            "depends_on": [{"task_key": "transform_sales"}],
            "notebook_task": {
                "notebook_path": "/course/notebooks/03_week/10_aggregations"
            }
        },
        {
            "task_key": "product_analytics",
            "depends_on": [{"task_key": "transform_sales"}],
            "notebook_task": {
                "notebook_path": "/course/notebooks/04_week/11_file_to_aggregation"
            }
        }
    ]
}

print("=== DAG Structure ===\n")
print("""
                    ┌─────────────────┐
                    │  ingest_sales   │
                    └────────┬────────┘
                             │
                    ┌────────┴────────┐
    ┌───────────────┤                 ├───────────────┐
    │               │                 │               │
    ▼               ▼                 ▼               ▼
┌────────┐  ┌────────────┐    ┌──────────────┐      
│ingest_ │  │ingest_     │    │ingest_       │
│customers│  │products    │    │              │
└───┬────┘  └─────┬──────┘    └──────┬───────┘
    │             │                   │
    └─────────────┴───────────────────┘
                  │
                  ▼
          ┌───────────────┐
          │transform_sales│
          └───────┬───────┘
                  │
         ┌────────┴────────┐
         │                 │
         ▼                 ▼
┌────────────────┐  ┌──────────────┐
│daily_          │  │product_      │
│aggregation     │  │analytics     │
└────────────────┘  └──────────────┘
""")

print("Task Execution Order:")
print("1. ingest_sales, ingest_customers, ingest_products (parallel)")
print("2. transform_sales (waits for all ingestion)")
print("3. daily_aggregation, product_analytics (parallel)")

print(f"\n Total tasks: {len(dag_job['tasks'])}")
