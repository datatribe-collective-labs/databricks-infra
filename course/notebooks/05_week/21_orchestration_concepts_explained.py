# Databricks notebook source
# MAGIC %md
# MAGIC # Job Orchestration Concepts Explained - Week 5
# MAGIC
# MAGIC This notebook provides a practical exploration of job orchestration concepts,
# MAGIC focusing on building reliable, maintainable, and scalable workflow automation.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand DAGs (Directed Acyclic Graphs) and workflow dependencies
# MAGIC - Learn task orchestration patterns
# MAGIC - Master retry and failure handling strategies
# MAGIC - Explore scheduling and triggering patterns
# MAGIC - Understand parameterization and dynamic workflows
# MAGIC
# MAGIC ## Topics Covered
# MAGIC
# MAGIC 1. DAGs and Task Dependencies
# MAGIC 2. Task Orchestration Patterns
# MAGIC 3. Retry and Failure Handling
# MAGIC 4. Scheduling and Triggering
# MAGIC 5. Parameterization and Dynamic Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DAGs and Task Dependencies
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **DAG (Directed Acyclic Graph)**: A workflow representation where:
# MAGIC - **Directed**: Tasks flow in one direction (A → B → C)
# MAGIC - **Acyclic**: No circular dependencies (no loops)
# MAGIC - **Graph**: Visual representation of task relationships
# MAGIC
# MAGIC **Why DAGs?**
# MAGIC - **Clarity**: Visual representation of workflow
# MAGIC - **Parallelism**: Execute independent tasks concurrently
# MAGIC - **Fault Tolerance**: Retry failed tasks without rerunning entire workflow
# MAGIC - **Debugging**: Identify bottlenecks and failures
# MAGIC
# MAGIC ### Dependency Types
# MAGIC
# MAGIC 1. **Sequential**: Task B waits for Task A to complete
# MAGIC 2. **Parallel**: Tasks A and B run simultaneously
# MAGIC 3. **Fan-out**: One task triggers multiple downstream tasks
# MAGIC 4. **Fan-in**: Multiple tasks feed into one downstream task

# COMMAND ----------

print("=== Understanding DAG Patterns ===\n")

# Example workflow: E-commerce Order Processing
print("""
E-commerce Order Processing DAG:

                    ┌─────────────────┐
                    │  Ingest Orders  │ (Task 1)
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
    ┌──────────────────┐         ┌──────────────────┐
    │ Validate Orders  │         │ Enrich Customer  │ (Tasks 2 & 3 - Parallel)
    └────────┬─────────┘         └────────┬─────────┘
             │                             │
             └──────────────┬──────────────┘
                            ▼
                  ┌──────────────────┐
                  │ Calculate Metrics│ (Task 4 - Fan-in)
                  └────────┬─────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ Update Dashboard │     │ Send Alerts      │ (Tasks 5 & 6 - Parallel)
    └──────────────────┘     └──────────────────┘

Task Dependencies:
  - Task 1 → Tasks 2, 3 (fan-out)
  - Tasks 2, 3 → Task 4 (fan-in)
  - Task 4 → Tasks 5, 6 (fan-out)
""")

print("✅ DAG benefits:")
print("  - Tasks 2 & 3 run in parallel (faster)")
print("  - Task 4 waits for both 2 & 3 (dependency)")
print("  - Clear execution flow")
print("  - Easy to debug failures")

# COMMAND ----------

# Simulate DAG execution order
from datetime import datetime
import time

print("=== DAG Execution Simulation ===\n")

def execute_task(task_name, duration=1):
    """Simulate task execution"""
    start = datetime.now()
    print(f"[{start.strftime('%H:%M:%S')}] Starting: {task_name}")
    time.sleep(duration)  # Simulate work
    end = datetime.now()
    print(f"[{end.strftime('%H:%M:%S')}] Completed: {task_name} (took {duration}s)")
    return {"task": task_name, "start": start, "end": end, "status": "SUCCESS"}

# Sequential execution (no DAG optimization)
print("WITHOUT DAG parallelism (sequential):")
print("-" * 50)
seq_start = datetime.now()

execute_task("Task 1: Ingest Orders", 2)
execute_task("Task 2: Validate Orders", 3)
execute_task("Task 3: Enrich Customer", 3)
execute_task("Task 4: Calculate Metrics", 2)
execute_task("Task 5: Update Dashboard", 1)
execute_task("Task 6: Send Alerts", 1)

seq_total = (datetime.now() - seq_start).total_seconds()
print(f"\nTotal sequential execution time: {seq_total}s")

# COMMAND ----------

print("\n\nWITH DAG parallelism (optimized):")
print("-" * 50)
dag_start = datetime.now()

# Task 1 (must run first)
execute_task("Task 1: Ingest Orders", 2)

# Tasks 2 & 3 (parallel - fan-out)
print("\n[PARALLEL EXECUTION: Tasks 2 & 3]")
execute_task("Task 2: Validate Orders", 3)
execute_task("Task 3: Enrich Customer", 3)
# In reality, these would run concurrently. Max time = max(3, 3) = 3s

# Task 4 (fan-in - waits for 2 & 3)
execute_task("Task 4: Calculate Metrics", 2)

# Tasks 5 & 6 (parallel - fan-out)
print("\n[PARALLEL EXECUTION: Tasks 5 & 6]")
execute_task("Task 5: Update Dashboard", 1)
execute_task("Task 6: Send Alerts", 1)
# In reality, these would run concurrently. Max time = max(1, 1) = 1s

dag_total = 2 + 3 + 2 + 1  # Optimized parallel execution
print(f"\nTotal DAG execution time (with parallelism): {dag_total}s")
print(f"⚡ Speedup: {seq_total / dag_total:.1f}x faster!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Task Orchestration Patterns
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Task Orchestration**: Coordinating multiple tasks in a workflow
# MAGIC
# MAGIC **Common Patterns:**
# MAGIC
# MAGIC 1. **Sequential Chain**: A → B → C (simple pipeline)
# MAGIC 2. **Branching**: Decision-based routing
# MAGIC 3. **Dynamic Task Generation**: Create tasks at runtime
# MAGIC 4. **Subworkflows**: Nested DAGs for modularity
# MAGIC 5. **Sensor/Trigger**: Wait for external events
# MAGIC
# MAGIC ### Databricks Job Orchestration
# MAGIC
# MAGIC - **Multi-task Jobs**: Define task dependencies in UI/API
# MAGIC - **Notebook Tasks**: Execute notebooks with parameters
# MAGIC - **Python/Jar Tasks**: Run Python wheels or JARs
# MAGIC - **Conditional Tasks**: if_condition for branching

# COMMAND ----------

print("=== Orchestration Pattern 1: Sequential Chain ===\n")

# Simple sequential pipeline
tasks_sequential = [
    {"name": "extract", "notebook": "01_extract_data"},
    {"name": "transform", "notebook": "02_transform_data", "depends_on": ["extract"]},
    {"name": "load", "notebook": "03_load_data", "depends_on": ["transform"]}
]

print("Sequential Pipeline:")
for task in tasks_sequential:
    depends = task.get("depends_on", [])
    if depends:
        print(f"  {task['name']} (depends on: {', '.join(depends)})")
    else:
        print(f"  {task['name']} (entry point)")

print("\nExecution flow: extract → transform → load")
print("✅ Simple, predictable, easy to debug")
print("⚠️ No parallelism, slower for large workflows")

# COMMAND ----------

print("=== Orchestration Pattern 2: Branching (Conditional) ===\n")

# Conditional branching based on data volume
def get_data_volume():
    """Simulate checking data volume"""
    return 1500  # MB

data_volume = get_data_volume()
print(f"Data volume: {data_volume} MB")

if data_volume > 1000:
    print("\n✅ Large dataset detected")
    print("  Route: extract → validate → large_cluster_transform → load")
    processing_mode = "large_cluster"
else:
    print("\n✅ Small dataset detected")
    print("  Route: extract → validate → small_cluster_transform → load")
    processing_mode = "small_cluster"

print(f"\nSelected processing mode: {processing_mode}")
print("\n✅ Conditional branching benefits:")
print("  - Cost optimization (right-size resources)")
print("  - Performance optimization")
print("  - Automatic decision-making")

# COMMAND ----------

print("=== Orchestration Pattern 3: Fan-out/Fan-in ===\n")

# Process multiple datasets in parallel, then aggregate
datasets = ["sales_data", "customer_data", "product_data", "inventory_data"]

print("Fan-out pattern (parallel processing):")
print("  Source → Multiple parallel tasks")
print()

for dataset in datasets:
    print(f"  ├─ Process {dataset} (parallel)")

print("\nFan-in pattern (aggregation):")
print("  All tasks → Single aggregation task")
print()
print("  │")
print("  └─→ Aggregate all results")

print("\n✅ Benefits:")
print("  - Parallel processing (4x faster)")
print("  - Independent failures (one dataset fails, others continue)")
print("  - Scalable (add more datasets easily)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Retry and Failure Handling
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Failure Handling**: How to respond when tasks fail
# MAGIC
# MAGIC **Retry Strategies:**
# MAGIC 1. **Immediate Retry**: Retry instantly (for transient errors)
# MAGIC 2. **Exponential Backoff**: Wait longer between retries
# MAGIC 3. **Max Retries**: Limit retry attempts
# MAGIC 4. **No Retry**: Fail immediately (for permanent errors)
# MAGIC
# MAGIC **Failure Responses:**
# MAGIC - **Fail Entire Job**: Critical task failure
# MAGIC - **Continue on Failure**: Non-critical task
# MAGIC - **Fallback Task**: Execute alternative task
# MAGIC - **Manual Intervention**: Alert and wait for human action

# COMMAND ----------

import random
import time

print("=== Retry Strategy: Exponential Backoff ===\n")

def unreliable_task(success_rate=0.3):
    """Simulates a task that might fail"""
    if random.random() < success_rate:
        return True
    else:
        raise Exception("Transient network error")

def execute_with_retry(task_func, max_retries=3, base_delay=1):
    """Execute task with exponential backoff retry"""
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries}...", end=" ")
            result = task_func()
            print("✅ SUCCESS")
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                print(f"❌ FAILED ({e}). Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"❌ FAILED after {max_retries} attempts")
                raise

# Simulate unreliable task
print("Executing unreliable task (30% success rate):")
try:
    execute_with_retry(unreliable_task, max_retries=3, base_delay=1)
except Exception as e:
    print(f"\n⚠️ Task failed permanently: {e}")

print("\n✅ Exponential backoff benefits:")
print("  - Attempt 1: Retry after 1s")
print("  - Attempt 2: Retry after 2s")
print("  - Attempt 3: Retry after 4s")
print("  - Gives system time to recover")

# COMMAND ----------

print("=== Failure Handling Pattern: Fallback Task ===\n")

def primary_data_source():
    """Primary data source (might fail)"""
    if random.random() < 0.3:
        return "Data from primary source"
    else:
        raise Exception("Primary source unavailable")

def fallback_data_source():
    """Backup data source (always available)"""
    return "Data from backup source"

# Try primary, fallback to secondary
print("Attempting to fetch data...")
try:
    data = primary_data_source()
    print(f"✅ {data}")
    source_used = "primary"
except Exception as e:
    print(f"⚠️ Primary failed ({e})")
    print("  Switching to fallback source...")
    data = fallback_data_source()
    print(f"✅ {data}")
    source_used = "fallback"

print(f"\n✅ Data source used: {source_used}")
print("✅ Workflow continued despite primary failure")

# COMMAND ----------

print("=== Failure Handling: Task Dependencies ===\n")

# Define task criticality
task_config = [
    {"name": "load_sales_data", "critical": True, "on_failure": "fail_job"},
    {"name": "load_marketing_data", "critical": False, "on_failure": "continue"},
    {"name": "enrich_with_weather", "critical": False, "on_failure": "skip_downstream"},
    {"name": "calculate_metrics", "critical": True, "on_failure": "fail_job"}
]

print("Task Configuration:")
print("-" * 60)
for task in task_config:
    critical = "CRITICAL" if task["critical"] else "OPTIONAL"
    print(f"  {task['name']}")
    print(f"    Criticality: {critical}")
    print(f"    On Failure: {task['on_failure']}")
    print()

print("Failure Handling Logic:")
print("-" * 60)
print("  ✅ load_sales_data fails → Entire job fails (critical)")
print("  ✅ load_marketing_data fails → Job continues (optional)")
print("  ✅ enrich_with_weather fails → Skip weather-dependent tasks")
print("  ✅ calculate_metrics fails → Entire job fails (critical)")

print("\n✅ Benefits:")
print("  - Critical failures stop job (prevent bad data)")
print("  - Optional failures don't block workflow")
print("  - Flexible failure handling per task")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Scheduling and Triggering
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Scheduling**: When and how often to run workflows
# MAGIC
# MAGIC **Trigger Types:**
# MAGIC 1. **Time-based (Cron)**: Run at specific times
# MAGIC 2. **Event-based**: Triggered by external events
# MAGIC 3. **File-based**: Run when file arrives
# MAGIC 4. **Manual**: User-initiated
# MAGIC 5. **Continuous**: Always running (streaming)
# MAGIC
# MAGIC **Scheduling Patterns:**
# MAGIC - **Batch Window**: Daily at 2 AM
# MAGIC - **Micro-batching**: Every 5 minutes
# MAGIC - **Real-time**: Continuous streaming
# MAGIC - **On-demand**: Manual trigger

# COMMAND ----------

print("=== Scheduling Pattern 1: Time-based (Cron) ===\n")

# Common cron patterns
cron_patterns = [
    ("0 2 * * *", "Daily at 2:00 AM", "Nightly ETL jobs"),
    ("0 */6 * * *", "Every 6 hours", "Periodic data refresh"),
    ("0 0 * * 0", "Weekly on Sunday midnight", "Weekly aggregations"),
    ("0 0 1 * *", "Monthly on 1st at midnight", "Monthly reports"),
    ("*/15 * * * *", "Every 15 minutes", "Near real-time updates")
]

print("Common Cron Schedules:")
print("-" * 70)
print(f"{'Cron Expression':<20} {'Description':<30} {'Use Case':<20}")
print("-" * 70)
for cron, desc, use_case in cron_patterns:
    print(f"{cron:<20} {desc:<30} {use_case:<20}")

print("\n✅ Time-based scheduling:")
print("  - Predictable execution")
print("  - Easy to plan maintenance windows")
print("  - Good for batch processing")

# COMMAND ----------

print("=== Scheduling Pattern 2: Event-based Triggering ===\n")

# Simulate event-based triggering
events = [
    {"event": "file_arrived", "file": "sales_2024_01_15.csv", "trigger": "sales_etl_job"},
    {"event": "api_webhook", "source": "payment_gateway", "trigger": "payment_processing_job"},
    {"event": "stream_message", "topic": "user_events", "trigger": "user_analytics_job"},
    {"event": "manual_trigger", "user": "data_engineer", "trigger": "adhoc_analysis_job"}
]

print("Event-based Triggers:")
print("-" * 70)
for event in events:
    print(f"Event: {event['event']}")
    print(f"  Source: {event.get('file', event.get('source', event.get('topic', event.get('user'))))}")
    print(f"  Triggers: {event['trigger']}")
    print()

print("✅ Event-based benefits:")
print("  - React immediately to events")
print("  - No unnecessary job runs")
print("  - Lower latency (faster processing)")

# COMMAND ----------

print("=== Scheduling Pattern 3: File Arrival Trigger ===\n")

# Simulate file watcher pattern
import os
from datetime import datetime

def check_for_new_files(directory="/tmp/landing_zone", processed_files=set()):
    """Check for new files to process"""
    # Simulate file arrival
    new_files = ["sales_2024_01_20.csv", "sales_2024_01_21.csv"]

    unprocessed = [f for f in new_files if f not in processed_files]
    return unprocessed

def process_file(filename):
    """Process a single file"""
    print(f"  ✅ Processing {filename}...")
    # Simulate processing
    return {"file": filename, "status": "success", "records": 1000}

# File watcher logic
print("File Watcher Pattern:")
print("-" * 60)
processed = set()

for iteration in range(3):
    print(f"\nIteration {iteration + 1}: Checking for new files...")
    new_files = check_for_new_files(processed_files=processed)

    if new_files:
        print(f"  Found {len(new_files)} new file(s)")
        for file in new_files:
            result = process_file(file)
            processed.add(file)
    else:
        print("  No new files")

    time.sleep(1)  # Wait before next check

print(f"\n✅ Total files processed: {len(processed)}")
print("✅ File-based triggering:")
print("  - Process files as they arrive")
print("  - Avoid reprocessing (track processed files)")
print("  - Scalable for high-volume ingestion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parameterization and Dynamic Workflows
# MAGIC
# MAGIC ### Logical Concept
# MAGIC
# MAGIC **Parameterization**: Make workflows flexible with runtime parameters
# MAGIC
# MAGIC **Parameter Types:**
# MAGIC 1. **Static**: Defined at design time
# MAGIC 2. **Runtime**: Passed when job starts
# MAGIC 3. **Environment**: Different per environment (dev/prod)
# MAGIC 4. **Dynamic**: Computed during execution
# MAGIC
# MAGIC **Use Cases:**
# MAGIC - Date ranges (process_date)
# MAGIC - Environment config (env=prod)
# MAGIC - Processing modes (full_load vs incremental)
# MAGIC - Resource sizing (cluster_size=large)

# COMMAND ----------

print("=== Parameterization Pattern 1: Date-based Processing ===\n")

from datetime import datetime, timedelta

# Simulate notebook parameters
dbutils.widgets.text("process_date", "")
dbutils.widgets.dropdown("mode", "incremental", ["full", "incremental"])

# Get parameters
process_date_str = dbutils.widgets.get("process_date")
mode = dbutils.widgets.get("mode")

# Default to yesterday if not provided
if not process_date_str:
    process_date = datetime.now() - timedelta(days=1)
else:
    process_date = datetime.strptime(process_date_str, "%Y-%m-%d")

print(f"Job Parameters:")
print(f"  process_date: {process_date.strftime('%Y-%m-%d')}")
print(f"  mode: {mode}")

# Use parameters to determine processing logic
if mode == "full":
    print(f"\n✅ Running FULL load for {process_date.strftime('%Y-%m-%d')}")
    print("  - Process all historical data")
    print("  - Overwrite existing data")
else:
    print(f"\n✅ Running INCREMENTAL load for {process_date.strftime('%Y-%m-%d')}")
    print("  - Process only new/changed records")
    print("  - Merge with existing data")

print("\n✅ Parameterization benefits:")
print("  - Reprocess any date on demand")
print("  - Switch between full/incremental modes")
print("  - One notebook, multiple use cases")

# COMMAND ----------

print("=== Parameterization Pattern 2: Environment-specific Config ===\n")

# Simulate environment parameter
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"])
environment = dbutils.widgets.get("environment")

# Environment-specific configuration
config = {
    "dev": {
        "catalog": "dev_catalog",
        "cluster_size": "Small",
        "checkpoint_location": "/tmp/dev/checkpoints",
        "alert_email": "dev-team@example.com"
    },
    "staging": {
        "catalog": "staging_catalog",
        "cluster_size": "Medium",
        "checkpoint_location": "/tmp/staging/checkpoints",
        "alert_email": "qa-team@example.com"
    },
    "prod": {
        "catalog": "prod_catalog",
        "cluster_size": "Large",
        "checkpoint_location": "/dbfs/prod/checkpoints",
        "alert_email": "data-platform@example.com"
    }
}

env_config = config[environment]

print(f"Environment: {environment.upper()}")
print("-" * 60)
for key, value in env_config.items():
    print(f"  {key}: {value}")

print("\n✅ Environment-specific benefits:")
print("  - Same code, different configs")
print("  - Prevent prod/dev data mixing")
print("  - Appropriate resources per environment")

# COMMAND ----------

print("=== Parameterization Pattern 3: Dynamic Task Generation ===\n")

# Dynamically generate tasks based on available datasets
available_datasets = ["sales", "marketing", "support", "product"]

print("Available datasets:", available_datasets)
print("\nDynamically generating processing tasks:")
print("-" * 60)

# Generate task for each dataset
generated_tasks = []
for dataset in available_datasets:
    task = {
        "task_key": f"process_{dataset}",
        "notebook_path": f"/Shared/process_dataset",
        "parameters": {
            "dataset_name": dataset,
            "target_schema": f"{dataset}_silver"
        }
    }
    generated_tasks.append(task)
    print(f"✅ Generated task: process_{dataset}")
    print(f"   Parameters: {task['parameters']}")

print(f"\nTotal tasks generated: {len(generated_tasks)}")

# Simulate DAG structure
print("\nDynamic DAG structure:")
print("  ingest_all_datasets")
print("      ├─→ process_sales")
print("      ├─→ process_marketing")
print("      ├─→ process_support")
print("      └─→ process_product")
print("  aggregate_results (depends on all process_* tasks)")

print("\n✅ Dynamic task generation benefits:")
print("  - Add new datasets without code changes")
print("  - Scales automatically")
print("  - Consistent processing logic")

# COMMAND ----------

print("=== Parameterization Pattern 4: Conditional Execution ===\n")

# Parameters that control execution flow
dbutils.widgets.dropdown("enable_validation", "true", ["true", "false"])
dbutils.widgets.dropdown("enable_alerts", "true", ["true", "false"])
dbutils.widgets.text("data_quality_threshold", "0.95")

enable_validation = dbutils.widgets.get("enable_validation") == "true"
enable_alerts = dbutils.widgets.get("enable_alerts") == "true"
quality_threshold = float(dbutils.widgets.get("data_quality_threshold"))

print("Execution Configuration:")
print(f"  enable_validation: {enable_validation}")
print(f"  enable_alerts: {enable_alerts}")
print(f"  data_quality_threshold: {quality_threshold}")

print("\nWorkflow execution plan:")
print("  1. Ingest data ✅ (always runs)")

if enable_validation:
    print("  2. Validate data ✅ (enabled)")
    print(f"     - Quality threshold: {quality_threshold * 100}%")
else:
    print("  2. Validate data ⏭️ (skipped)")

print("  3. Transform data ✅ (always runs)")

if enable_alerts:
    print("  4. Send alerts ✅ (enabled)")
else:
    print("  4. Send alerts ⏭️ (skipped)")

print("\n✅ Conditional execution benefits:")
print("  - Toggle features on/off")
print("  - A/B testing different approaches")
print("  - Debug mode (skip expensive steps)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Key Takeaways
# MAGIC
# MAGIC ### 1. DAGs and Dependencies
# MAGIC
# MAGIC - **DAG**: Directed Acyclic Graph for workflow representation
# MAGIC - **Parallelism**: Run independent tasks concurrently
# MAGIC - **Dependencies**: Sequential, fan-out, fan-in patterns
# MAGIC - **Benefits**: Faster execution, clear flow, easy debugging
# MAGIC
# MAGIC ### 2. Task Orchestration
# MAGIC
# MAGIC - **Sequential**: Simple A → B → C pipeline
# MAGIC - **Branching**: Conditional routing based on logic
# MAGIC - **Fan-out/Fan-in**: Parallel processing + aggregation
# MAGIC - **Databricks Jobs**: Multi-task jobs with dependencies
# MAGIC
# MAGIC ### 3. Retry and Failure Handling
# MAGIC
# MAGIC - **Retry strategies**:
# MAGIC   - Immediate retry (transient errors)
# MAGIC   - Exponential backoff (system recovery)
# MAGIC   - Max retries (prevent infinite loops)
# MAGIC - **Failure responses**:
# MAGIC   - Fail job (critical tasks)
# MAGIC   - Continue (optional tasks)
# MAGIC   - Fallback (alternative execution)
# MAGIC
# MAGIC ### 4. Scheduling and Triggering
# MAGIC
# MAGIC - **Time-based**: Cron schedules (daily, hourly, etc.)
# MAGIC - **Event-based**: React to external events
# MAGIC - **File-based**: Process files as they arrive
# MAGIC - **Manual**: On-demand execution
# MAGIC
# MAGIC ### 5. Parameterization
# MAGIC
# MAGIC - **Date parameters**: Reprocess any date range
# MAGIC - **Environment config**: Dev/staging/prod settings
# MAGIC - **Dynamic tasks**: Generate tasks at runtime
# MAGIC - **Conditional execution**: Toggle features on/off
# MAGIC
# MAGIC ### Production Orchestration Checklist
# MAGIC
# MAGIC - ✅ Design DAG with clear dependencies
# MAGIC - ✅ Maximize parallelism (fan-out/fan-in)
# MAGIC - ✅ Implement retry with exponential backoff
# MAGIC - ✅ Define failure handling per task criticality
# MAGIC - ✅ Choose appropriate trigger (time/event/file)
# MAGIC - ✅ Parameterize for flexibility
# MAGIC - ✅ Use environment-specific configs
# MAGIC - ✅ Monitor execution metrics
# MAGIC - ✅ Set up alerting on failures
# MAGIC - ✅ Document workflow and dependencies
# MAGIC
# MAGIC ### Databricks Jobs Best Practices
# MAGIC
# MAGIC - ✅ Use multi-task jobs (not separate jobs)
# MAGIC - ✅ Define task dependencies explicitly
# MAGIC - ✅ Set retry policies per task
# MAGIC - ✅ Use job parameters for flexibility
# MAGIC - ✅ Enable email/webhook alerts
# MAGIC - ✅ Use cluster pools for faster startup
# MAGIC - ✅ Tag jobs for cost tracking
# MAGIC - ✅ Version control job definitions (JSON)
# MAGIC - ✅ Test in dev before deploying to prod
# MAGIC - ✅ Monitor with Databricks Jobs UI
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Create multi-task Databricks jobs
# MAGIC - Implement complex DAG workflows
# MAGIC - Practice failure recovery scenarios
# MAGIC - Set up production scheduling
# MAGIC - Build parameterized, reusable workflows

# COMMAND ----------

# Clean up widgets
dbutils.widgets.removeAll()
print("=== Cleanup Complete ===")
print("All widgets removed")