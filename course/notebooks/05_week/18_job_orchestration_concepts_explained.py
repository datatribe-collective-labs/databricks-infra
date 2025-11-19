# Databricks notebook source
# MAGIC %md
# MAGIC # Job Orchestration Concepts & Implementation Guide - Week 5
# MAGIC
# MAGIC This notebook provides comprehensive coverage of job orchestration in Databricks,
# MAGIC combining conceptual foundations with practical implementation guides.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Understand DAGs (Directed Acyclic Graphs) and workflow dependencies
# MAGIC - Learn task orchestration patterns and best practices
# MAGIC - Master retry and failure handling strategies
# MAGIC - Explore scheduling and triggering patterns
# MAGIC - **Create jobs using Databricks UI** (step-by-step guide)
# MAGIC - **Create jobs programmatically** using Databricks SDK
# MAGIC
# MAGIC ## Topics Covered
# MAGIC
# MAGIC ### Part 1: Conceptual Foundations
# MAGIC 1. DAGs and Task Dependencies
# MAGIC 2. Task Orchestration Patterns
# MAGIC 3. Retry and Failure Handling
# MAGIC 4. Scheduling and Triggering
# MAGIC 5. Parameterization and Dynamic Workflows
# MAGIC
# MAGIC ### Part 2: Creating Jobs - UI Approach
# MAGIC 6. Step-by-Step UI Job Creation
# MAGIC 7. Configuring Multi-Task Jobs in UI
# MAGIC 8. Managing Job Runs and Monitoring
# MAGIC
# MAGIC ### Part 3: Creating Jobs - Programmatic Approach
# MAGIC 9. Using Databricks SDK
# MAGIC 10. Job Templates and Automation
# MAGIC 11. CI/CD Integration Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 1: CONCEPTUAL FOUNDATIONS
# MAGIC ---

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

# Theoretical parallel execution time
parallel_time = 2 + 3 + 2 + 1  # Task1(2s) + max(Task2,Task3)(3s) + Task4(2s) + max(Task5,Task6)(1s)

print("""
Execution Plan:
  Phase 1: Task 1 (2s)
  Phase 2: Tasks 2 & 3 in parallel (3s max)
  Phase 3: Task 4 (2s)
  Phase 4: Tasks 5 & 6 in parallel (1s max)

Total time: 2 + 3 + 2 + 1 = 8 seconds
""")

print(f"Sequential time: {seq_total}s")
print(f"Parallel time: {parallel_time}s")
print(f"⚡ Speedup: {seq_total / parallel_time:.1f}x faster!")

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

print("=== Orchestration Pattern 2: Fan-out/Fan-in ===\n")

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

print("=== Failure Handling: Task Criticality ===\n")

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

print("=== Scheduling Pattern: Time-based (Cron) ===\n")

# Common cron patterns
cron_patterns = [
    ("0 2 * * *", "Daily at 2:00 AM", "Nightly ETL jobs"),
    ("0 */6 * * *", "Every 6 hours", "Periodic data refresh"),
    ("0 0 * * 0", "Weekly on Sunday midnight", "Weekly aggregations"),
    ("0 0 2 1 * ?", "Monthly on 1st at 2 AM", "Monthly reports"),
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

from datetime import datetime, timedelta

print("=== Parameterization Pattern: Date-based Processing ===\n")

# Simulate job parameters
process_date_param = "2024-01-20"  # In real job, this would come from dbutils.widgets
mode_param = "incremental"  # full or incremental

process_date = datetime.strptime(process_date_param, "%Y-%m-%d")

print(f"Job Parameters:")
print(f"  process_date: {process_date.strftime('%Y-%m-%d')}")
print(f"  mode: {mode_param}")

# Use parameters to determine processing logic
if mode_param == "full":
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
print("  - One job, multiple use cases")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 2: CREATING JOBS - UI APPROACH
# MAGIC ---
# MAGIC
# MAGIC ## 6. Step-by-Step UI Job Creation
# MAGIC
# MAGIC Creating jobs in the Databricks UI is the most user-friendly approach and perfect for:
# MAGIC - Learning and experimentation
# MAGIC - Quick job setup
# MAGIC - Visual understanding of workflows
# MAGIC - Non-technical stakeholders
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Access to a Databricks workspace
# MAGIC - Notebooks ready to be orchestrated
# MAGIC - Appropriate permissions (Can Manage Jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Navigate to Workflows
# MAGIC
# MAGIC ```
# MAGIC 1. Log into your Databricks workspace
# MAGIC 2. Click "Workflows" in the left sidebar
# MAGIC 3. Click "Create Job" button (top right)
# MAGIC ```
# MAGIC
# MAGIC **Screenshot location:** Workflows → Create Job
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Configure Job Basics
# MAGIC
# MAGIC **Job Settings:**
# MAGIC ```
# MAGIC Job Name: Sales Data Pipeline
# MAGIC Description: Daily sales data processing (Bronze → Silver → Gold)
# MAGIC Tags: production, sales, etl
# MAGIC ```
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Use descriptive names (include purpose and frequency)
# MAGIC - Add meaningful descriptions
# MAGIC - Tag jobs for organization and cost tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Add First Task
# MAGIC
# MAGIC **Click "Add task" and configure:**
# MAGIC
# MAGIC ```
# MAGIC Task name: bronze_file_ingestion
# MAGIC Type: Notebook
# MAGIC Source: Workspace
# MAGIC Path: /Workspace/course/notebooks/02_week/06_file_ingestion # Modify with your notebook path
# MAGIC Cluster: [Select or create cluster. Remember that serverless is fully managed.]
# MAGIC ```
# MAGIC
# MAGIC **Cluster Options:**
# MAGIC - **Existing cluster**: Faster start (if already running)
# MAGIC - **New cluster**: Isolated resources per job
# MAGIC - **Serverless**: Fastest start, auto-scaling (recommended)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Configure Cluster (if using "New cluster")
# MAGIC
# MAGIC **Cluster Configuration:**
# MAGIC ```
# MAGIC Cluster mode: Standard
# MAGIC Databricks runtime: 14.3 LTS
# MAGIC Node type: Standard_DS3_v2 (or similar)
# MAGIC Workers: 2 (autoscaling 2-8 recommended)
# MAGIC ```
# MAGIC
# MAGIC **Cost Optimization Tips:**
# MAGIC - Use autoscaling clusters
# MAGIC - Enable auto-termination (15-30 minutes)
# MAGIC - Use spot instances for non-critical jobs
# MAGIC - Consider cluster pools for faster startup
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5: Add Task Parameters (Optional)
# MAGIC
# MAGIC **Parameters Section:**
# MAGIC ```
# MAGIC Key: catalog          Value: databricks_course
# MAGIC Key: schema           Value: your_schema_name
# MAGIC Key: process_date     Value: {{job.start_time.date}}
# MAGIC ```
# MAGIC
# MAGIC **Parameter Variables:**
# MAGIC - `{{job.start_time}}` - Job start timestamp
# MAGIC - `{{job.trigger}}` - How job was triggered
# MAGIC - `{{job.id}}` - Unique job ID
# MAGIC - `{{job.run_id}}` - Unique run ID
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 6: Configure Retries and Timeout
# MAGIC
# MAGIC **Advanced Settings:**
# MAGIC ```
# MAGIC Max concurrent runs: 1
# MAGIC Retry on failure: 3 times
# MAGIC Timeout: 2 hours
# MAGIC Email notifications: [your-email@example.com]
# MAGIC ```
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Set retries for transient failures (2-3 times)
# MAGIC - Use timeouts to prevent runaway jobs
# MAGIC - Configure email alerts for failures
# MAGIC - Limit concurrent runs to avoid resource conflicts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Configuring Multi-Task Jobs in UI
# MAGIC
# MAGIC Multi-task jobs allow you to orchestrate complex workflows with dependencies.
# MAGIC
# MAGIC ### Adding Additional Tasks
# MAGIC
# MAGIC **Task 2: API Ingestion (Parallel to Task 1)**
# MAGIC ```
# MAGIC 1. Click "+ Task" button
# MAGIC 2. Task name: bronze_api_ingestion
# MAGIC 3. Type: Notebook
# MAGIC 4. Path: /Workspace/course/notebooks/02_week/07_api_ingest
# MAGIC 5. Dependencies: None (runs in parallel with task 1)
# MAGIC 6. Cluster: Use same cluster as Task 1 (recommended)
# MAGIC ```
# MAGIC
# MAGIC **Task 3: Database Ingestion (Parallel)**
# MAGIC ```
# MAGIC Task name: bronze_database_ingestion
# MAGIC Path: /Workspace/course/notebooks/02_week/08_database_ingest # Modify with your notebook path
# MAGIC Dependencies: None
# MAGIC ```
# MAGIC
# MAGIC **Task 4: S3 Ingestion (Parallel)**
# MAGIC ```
# MAGIC Task name: bronze_s3_ingestion
# MAGIC Path: /Workspace/course/notebooks/02_week/09_s3_ingest # Modify with your notebook path
# MAGIC Dependencies: None
# MAGIC ```
# MAGIC
# MAGIC **Task 5: Validation (Depends on all ingestion tasks)**
# MAGIC ```
# MAGIC Task name: validate_ingestion
# MAGIC Path: /Workspace/course/notebooks/custom/validate_bronze # Modify with your notebook path
# MAGIC Dependencies:
# MAGIC   - bronze_file_ingestion
# MAGIC   - bronze_api_ingestion
# MAGIC   - bronze_database_ingestion
# MAGIC   - bronze_s3_ingestion
# MAGIC ```
# MAGIC
# MAGIC ### Visual DAG in UI
# MAGIC
# MAGIC The Databricks UI automatically generates a visual DAG showing:
# MAGIC - Task execution order
# MAGIC - Parallel vs sequential execution
# MAGIC - Dependency relationships
# MAGIC - Estimated execution time
# MAGIC
# MAGIC **Viewing the DAG:**
# MAGIC ```
# MAGIC 1. Go to the job details page
# MAGIC 2. Click "View DAG" or switch to "Graph" view
# MAGIC 3. Nodes represent tasks, edges represent dependencies
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Scheduling and Running Jobs
# MAGIC
# MAGIC ### Setting up a Schedule
# MAGIC
# MAGIC **1. Navigate to Schedule Tab**
# MAGIC ```
# MAGIC Job Details → Schedule → Add Schedule
# MAGIC ```
# MAGIC
# MAGIC **2. Configure Schedule**
# MAGIC ```
# MAGIC Schedule type: Cron
# MAGIC Cron expression: 0 0 2 * * ?  (Daily at 2 AM) # Quartz Cron Expression!
# MAGIC Timezone: America/New_York
# MAGIC Pause status: Active
# MAGIC ```
# MAGIC
# MAGIC **Common Schedules:**
# MAGIC - Daily at 2 AM: `0 0 2 * * ?`
# MAGIC - Every 6 hours: `0 0 */6 * * ?`
# MAGIC - Weekly Sunday 2 AM: `0 0 2 ? * SUN`
# MAGIC - Every 15 minutes: `0 */15 * * * ?`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Manual Job Runs
# MAGIC
# MAGIC **Run Job Immediately:**
# MAGIC ```
# MAGIC 1. Navigate to job details
# MAGIC 2. Click "Run now" button (top right)
# MAGIC 3. Optionally override parameters
# MAGIC 4. Click "Confirm"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Monitoring Job Runs
# MAGIC
# MAGIC **Run History:**
# MAGIC ```
# MAGIC Job Details → Runs tab
# MAGIC ```
# MAGIC
# MAGIC **For Each Run, You Can See:**
# MAGIC - Start time, duration, status
# MAGIC - Task-level execution details
# MAGIC - Spark UI link for performance analysis
# MAGIC - Logs for each task
# MAGIC - Output and error messages
# MAGIC
# MAGIC **Status Indicators:**
# MAGIC - 🟢 **Success**: All tasks completed
# MAGIC - 🔴 **Failed**: One or more tasks failed
# MAGIC - 🟡 **Running**: Currently executing
# MAGIC - ⚪ **Pending**: Waiting to start
# MAGIC - ⏸️ **Skipped**: Task skipped due to conditions
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Debugging Failed Runs
# MAGIC
# MAGIC **Step 1: Identify Failed Task**
# MAGIC ```
# MAGIC 1. Click on failed run
# MAGIC 2. Look for red status indicator on task
# MAGIC 3. Click on failed task
# MAGIC ```
# MAGIC
# MAGIC **Step 2: Review Logs**
# MAGIC ```
# MAGIC 1. Click "View Logs" or "Output" tab
# MAGIC 2. Look for error messages and stack traces
# MAGIC 3. Check stderr for Python errors
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Repair Run**
# MAGIC ```
# MAGIC 1. Fix the issue (code, permissions, data)
# MAGIC 2. Click "Repair run" button
# MAGIC 3. Select which tasks to re-run
# MAGIC 4. Only failed and downstream tasks will re-execute
# MAGIC ```

# COMMAND ----------

print("=== UI Job Creation Summary ===\n")

print("""
✅ ADVANTAGES OF UI APPROACH:
  - Visual, intuitive interface
  - No coding required for job setup
  - Immediate visual feedback (DAG view)
  - Easy to modify and experiment
  - Great for learning and prototyping
  - Built-in monitoring and debugging tools

⚠️ LIMITATIONS:
  - Manual creation (not version controlled)
  - Difficult to replicate across environments
  - No automation for job deployment
  - Hard to manage many jobs at scale

💡 BEST FOR:
  - Learning Databricks Jobs
  - Quick prototyping
  - Small number of jobs
  - Teams preferring visual tools
  - Ad-hoc analysis workflows
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 3: CREATING JOBS - PROGRAMMATIC APPROACH
# MAGIC ---
# MAGIC
# MAGIC ## 9. Using Databricks SDK
# MAGIC
# MAGIC The Databricks SDK allows you to create, manage, and run jobs programmatically.
# MAGIC This is essential for:
# MAGIC - **CI/CD pipelines**: Automated deployment
# MAGIC - **Infrastructure as Code**: Version-controlled job definitions
# MAGIC - **Dynamic job creation**: Generate jobs based on data/config
# MAGIC - **Multi-environment management**: Dev, staging, production

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Databricks SDK
# MAGIC
# MAGIC ```python
# MAGIC # On your local machine or in notebook
# MAGIC %pip install databricks-sdk
# MAGIC ```
# MAGIC ```python
# MAGIC # On poetry managed environment
# MAGIC %poetry add databricks-sdk
# MAGIC ```
# MAGIC
# MAGIC **Authentication Options:**
# MAGIC
# MAGIC 1. **Personal Access Token (PAT)**
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient(
# MAGIC     host="https://your-workspace.cloud.databricks.com",
# MAGIC     token="dapi..."
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 2. **OAuth (Recommended for Production)**
# MAGIC ```python
# MAGIC w = WorkspaceClient()  # Uses OAuth flow
# MAGIC ```
# MAGIC
# MAGIC 3. **Environment Variables**
# MAGIC ```bash
# MAGIC export DATABRICKS_HOST="https://..."
# MAGIC export DATABRICKS_TOKEN="dapi..."
# MAGIC ```
# MAGIC
# MAGIC 4. **Within Databricks Notebook (Automatic)**
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()  # Automatically authenticated
# MAGIC ```

# COMMAND ----------

# Install SDK (if not already installed)
%pip install databricks-sdk --quiet

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Initialize client (automatically authenticated within Databricks)
w = WorkspaceClient()

print("=== Databricks SDK Initialized ===")
print(f"Workspace host: {w.config.host}")
print(f"Current user: {w.current_user.me().user_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Simple Single-Task Job

# COMMAND ----------

from databricks.sdk.service.jobs import Task, NotebookTask, Source

print("=== Creating Single-Task Job ===\n")

# Define job configuration
job_name = "SDK_Demo_Simple_Ingestion"
notebook_path = "/Workspace/Shared/terraform-managed/course/notebooks/02_week/06_file_ingestion.py"

try:
    # Create job
    created_job = w.jobs.create(
        name=job_name,
        tasks=[
            Task(
                task_key="file_ingestion",
                description="Ingest files to Bronze layer",
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    source=Source.WORKSPACE,
                    base_parameters={
                        "catalog": "{CATALOG}",
                        "schema": "{USER_SCHEMA}"
                    }
                ),
                timeout_seconds=3600,  # 1 hour timeout
                max_retries=2
            )
        ],
        tags={
            "environment": "dev",
            "team": "data-engineering",
            "created_by": "sdk"
        }
    )

    print(f"✅ Job created successfully!")
    print(f"   Job ID: {created_job.job_id}")
    print(f"   Job Name: {job_name}")
    print(f"   View in UI: {w.config.host}/#job/{created_job.job_id}")

except Exception as e:
    print(f"❌ Error creating job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Multi-Task Job (4 Parallel Ingestion Tasks)
# MAGIC
# MAGIC This example recreates the ingestion pipeline with notebooks 06-09 running in parallel.

# COMMAND ----------

from databricks.sdk.service.compute import AutoScale, ClusterSpec
from databricks.sdk.service.jobs import JobCluster

print("=== Creating Multi-Task Ingestion Job ===\n")

multi_task_job_name = "SDK_Demo_Multi_Source_Ingestion"

# Define tasks
tasks = [
    Task(
        task_key="ingest_files",
        description="Ingest CSV/JSON/Parquet files",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/06_file_ingestion.py",
            source=Source.WORKSPACE,
            base_parameters={"catalog": "{CATALOG}"}
        ),
        timeout_seconds=3600,
        max_retries=2
    ),
    Task(
        task_key="ingest_api",
        description="Ingest data from REST APIs",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/07_api_ingest.py",
            source=Source.WORKSPACE,
            base_parameters={"catalog": "{CATALOG}"}
        ),
        timeout_seconds=3600,
        max_retries=2
    ),
    Task(
        task_key="ingest_database",
        description="Ingest data from databases",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/08_database_ingest.py",
            source=Source.WORKSPACE,
            base_parameters={"catalog": "{CATALOG}"}
        ),
        timeout_seconds=3600,
        max_retries=2
    ),
    Task(
        task_key="ingest_s3",
        description="Ingest data from S3/cloud storage",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/09_s3_ingest.py",
            source=Source.WORKSPACE,
            base_parameters={"catalog": "{CATALOG}"}
        ),
        timeout_seconds=3600,
        max_retries=2
    )
]

try:
    # Create multi-task job
    created_multi_job = w.jobs.create(
        name=multi_task_job_name,
        tasks=tasks,
        tags={
            "environment": "dev",
            "pipeline": "bronze_ingestion",
            "created_by": "sdk"
        },
        timeout_seconds=7200,  # 2 hour job timeout
        max_concurrent_runs=1
    )

    print(f"✅ Multi-task job created successfully!")
    print(f"   Job ID: {created_multi_job.job_id}")
    print(f"   Job Name: {multi_task_job_name}")
    print(f"   Tasks: {len(tasks)} parallel ingestion tasks")
    print(f"   View in UI: {w.config.host}/#job/{created_multi_job.job_id}")

    # Store job ID for later use
    demo_job_id = created_multi_job.job_id

except Exception as e:
    print(f"❌ Error creating multi-task job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running a Job Programmatically

# COMMAND ----------

print("=== Running Job via SDK ===\n")

try:
    # Trigger job run
    run = w.jobs.run_now(job_id=demo_job_id)

    print(f"✅ Job run triggered!")
    print(f"   Run ID: {run.run_id}")
    print(f"   View run: {w.config.host}/#job/{demo_job_id}/run/{run.run_id}")

    # Store run ID for monitoring
    demo_run_id = run.run_id

except Exception as e:
    print(f"❌ Error running job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring Job Execution

# COMMAND ----------

import time

print("=== Monitoring Job Execution ===\n")

try:
    # Get run status
    run_status = w.jobs.get_run(run_id=demo_run_id)

    print(f"Run ID: {run_status.run_id}")
    print(f"State: {run_status.state.life_cycle_state}")
    print(f"Result state: {run_status.state.result_state}")

    # Monitor until completion (for demonstration)
    print("\nMonitoring progress...")
    max_wait = 60  # Wait up to 60 seconds for demo
    waited = 0

    while waited < max_wait:
        run_status = w.jobs.get_run(run_id=demo_run_id)
        state = run_status.state.life_cycle_state

        print(f"  [{waited}s] State: {state}")

        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            result = run_status.state.result_state
            print(f"\n✅ Job completed with result: {result}")
            break

        time.sleep(5)
        waited += 5

    # Print task details
    print("\nTask Details:")
    for task_run in run_status.tasks:
        print(f"  Task: {task_run.task_key}")
        print(f"    State: {task_run.state.life_cycle_state}")
        if task_run.state.result_state:
            print(f"    Result: {task_run.state.result_state}")

except Exception as e:
    print(f"❌ Error monitoring job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating an Existing Job

# COMMAND ----------

print("=== Updating Job Configuration ===\n")

try:
    # Update job with schedule
    from databricks.sdk.service.jobs import CronSchedule, PauseStatus

    w.jobs.update(
        job_id=demo_job_id,
        new_settings=jobs.JobSettings(
            name=multi_task_job_name,
            tasks=tasks,
            #job_clusters=[job_cluster],
            schedule=CronSchedule(
                quartz_cron_expression="0 0 2 * * ?",  # Daily at 2AM, quartz style is different from traditionalcron
                timezone_id="America/New_York",
                pause_status=PauseStatus.PAUSED  # Start paused
            ),
            tags={
                "environment": "dev",
                "pipeline": "bronze_ingestion",
                "updated_by": "sdk",
                "schedule": "daily_2am"
            }
        )
    )

    print(f"✅ Job updated successfully!")
    print(f"   Added daily schedule: 2:00 AM EST")
    print(f"   Schedule status: PAUSED (activate manually in UI)")

except Exception as e:
    print(f"❌ Error updating job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listing All Jobs

# COMMAND ----------

print("=== Listing Jobs ===\n")

try:
    # List jobs with filter
    jobs_list = w.jobs.list(name=None, expand_tasks=False)

    print("Recent Jobs:")
    print("-" * 80)

    count = 0
    for job in jobs_list:
        if count >= 10:  # Show first 10 jobs
            break

        print(f"Job ID: {job.job_id}")
        print(f"  Name: {job.settings.name}")
        print(f"  Tasks: {len(job.settings.tasks) if job.settings.tasks else 0}")
        print(f"  Schedule: {job.settings.schedule.quartz_cron_expression if job.settings.schedule else 'None'}")
        print()

        count += 1

    print(f"Total jobs shown: {count}")

except Exception as e:
    print(f"❌ Error listing jobs: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deleting a Job (Cleanup)

# COMMAND ----------

print("=== Cleanup: Deleting Demo Jobs ===\n")

# Uncomment to actually delete the demo jobs
# try:
#     w.jobs.delete(job_id=demo_job_id)
#     print(f"✅ Deleted job: {demo_job_id}")
# except Exception as e:
#     print(f"❌ Error deleting job: {e}")

print("⚠️ Delete commands are commented out to prevent accidental deletion")
print("   Uncomment the code above to clean up demo jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Job Templates and Automation
# MAGIC
# MAGIC Creating reusable job templates for common patterns.

# COMMAND ----------

print("=== Reusable Job Template Function ===\n")

def create_etl_job(
    job_name: str,
    notebooks: list,
    catalog: str,
    schema: str,
    schedule_cron: str = None,
    environment: str = "dev"
):
    """
    Create a standard ETL job with multiple notebook tasks.

    Args:
        job_name: Name for the job
        notebooks: List of dict with 'task_key' and 'notebook_path'
        catalog: Unity Catalog name
        schema: Schema name
        schedule_cron: Optional cron expression
        environment: Environment tag (dev/staging/prod)

    Returns:
        Created job object
    """
    from databricks.sdk.service.jobs import Task, NotebookTask, Source
    from databricks.sdk.service.jobs import CronSchedule

    # Create tasks from notebook list
    tasks = []
    for idx, nb in enumerate(notebooks):
        tasks.append(
            Task(
                task_key=nb['task_key'],
                description=nb.get('description', f"Task {idx+1}"),
                notebook_task=NotebookTask(
                    notebook_path=nb['notebook_path'], # Relative path to defined notebook path, defined in workspace
                    source=Source.WORKSPACE,
                    base_parameters={
                        "catalog": catalog,
                        "schema": schema,
                        "environment": environment #dev/staging/prod
                    }
                ),
                depends_on=nb.get('depends_on', []),
                timeout_seconds=3600,
                max_retries=2
            )
        )

    # Build job settings
    job_settings = {
        "name": job_name,
        "tasks": tasks,
        "tags": {
            "environment": environment,
            "managed_by": "template"
        },
        "max_concurrent_runs": 1
    }

    # Add schedule if provided
    if schedule_cron:
        job_settings["schedule"] = CronSchedule(
            quartz_cron_expression=schedule_cron,
            timezone_id="America/New_York"
        )

    # Create job
    created_job = w.jobs.create(**job_settings)

    return created_job

# Example usage
example_notebooks = [
    {
        "task_key": "bronze_ingestion",
        "notebook_path": "/Workspace/course/notebooks//Workspace/Shared/terraform-managed/course/notebooks/02_week/06_file_ingestion.py",
        "description": "Ingest raw data"
    },
    {
        "task_key": "silver_transformation",
        "notebook_path": "/Workspace/Shared/terraform-managed/course/notebooks/03_week/11_simple_transformations.py", # 
        "description": "Transform to silver",
        "depends_on": [{"task_key": "bronze_ingestion"}]
    }
]

print("Template function created!")
print("\nExample usage:")
print("""
job = create_etl_job(
    job_name="My_ETL_Pipeline",
    notebooks=example_notebooks,
    catalog="databricks_course",
    schema="my_schema",
    schedule_cron="0 0 2 * * ?",
    environment="dev"
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. CI/CD Integration Patterns
# MAGIC
# MAGIC ### Job Configuration as Code (JSON/YAML)
# MAGIC
# MAGIC **Example: Job definition in JSON**
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "Production_Sales_Pipeline",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "ingest_sales",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Production/sales/01_ingest",
# MAGIC         "source": "WORKSPACE",
# MAGIC         "base_parameters": {
# MAGIC           "catalog": "prod_catalog",
# MAGIC           "date": "{{job.start_time.date}}"
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 2 * * ?",
# MAGIC     "timezone_id": "America/New_York"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### GitHub Actions Integration
# MAGIC
# MAGIC **Example workflow file: `.github/workflows/deploy-jobs.yml`**
# MAGIC ```yaml
# MAGIC name: Deploy Databricks Jobs
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v2
# MAGIC
# MAGIC       - name: Install Databricks CLI
# MAGIC         run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# MAGIC         
# MAGIC              echo "$HOME/.databricks/bin" >> $GITHUB_PATH
# MAGIC
# MAGIC       - name: Deploy Jobs
# MAGIC         env:
# MAGIC           DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
# MAGIC           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
# MAGIC         run: |
# MAGIC           python scripts/deploy_jobs.py --env production
# MAGIC ```
# MAGIC
# MAGIC ### Best Practices for CI/CD
# MAGIC
# MAGIC 1. **Version Control**: Store job definitions in Git
# MAGIC 2. **Environment Separation**: Different configs for dev/staging/prod
# MAGIC 3. **Automated Testing**: Test notebooks before deploying jobs
# MAGIC 4. **Idempotent Deployments**: Update existing jobs, don't duplicate
# MAGIC 5. **Rollback Strategy**: Keep previous job versions
# MAGIC 6. **Secret Management**: Use secret scopes, not hardcoded values

# COMMAND ----------

print("=== CI/CD Integration Example ===\n")

def deploy_job_from_config(config_file_path: str, environment: str):
    """
    Deploy job from JSON configuration file.

    Args:
        config_file_path: Path to job config JSON
        environment: Target environment (dev/staging/prod)

    Returns:
        Job ID
    """
    import json

    # Load configuration
    with open(config_file_path, 'r') as f:
        job_config = json.load(f)

    # Apply environment-specific overrides
    env_overrides = {
        "dev": {"max_concurrent_runs": 3},
        "staging": {"max_concurrent_runs": 2},
        "prod": {"max_concurrent_runs": 1}
    }

    job_config.update(env_overrides.get(environment, {}))
    job_config["tags"]["environment"] = environment

    # Check if job exists (by name)
    existing_jobs = list(w.jobs.list(name=job_config["name"]))

    if existing_jobs:
        # Update existing job
        job_id = existing_jobs[0].job_id
        w.jobs.update(job_id=job_id, new_settings=jobs.JobSettings(**job_config))
        print(f"✅ Updated existing job: {job_id}")
    else:
        # Create new job
        created_job = w.jobs.create(**job_config)
        job_id = created_job.job_id
        print(f"✅ Created new job: {job_id}")

    return job_id

print("CI/CD deployment function created!")
print("\nUsage in deployment script:")
print("""
# In your CI/CD pipeline
job_id = deploy_job_from_config(
    config_file_path="jobs/sales_pipeline.json",
    environment="production"
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary and Key Takeaways
# MAGIC ---

# COMMAND ----------

print("=== Complete Job Orchestration Guide - Summary ===\n")

print("""
📚 PART 1: CONCEPTUAL FOUNDATIONS
  ✅ DAGs and Dependencies - Parallel execution, task relationships
  ✅ Orchestration Patterns - Sequential, fan-out/fan-in, branching
  ✅ Retry & Failure Handling - Exponential backoff, task criticality
  ✅ Scheduling - Cron, event-based, file-based triggers
  ✅ Parameterization - Runtime flexibility, environment configs

🖱️ PART 2: UI APPROACH
  ✅ Step-by-step job creation in Databricks UI
  ✅ Multi-task job configuration with visual DAG
  ✅ Scheduling and monitoring in UI
  ✅ Debugging and repair runs

  💡 Best for: Learning, prototyping, visual preference
  ⚠️ Limitations: Manual, not version controlled

💻 PART 3: PROGRAMMATIC APPROACH (SDK)
  ✅ Databricks SDK setup and authentication
  ✅ Creating single and multi-task jobs programmatically
  ✅ Running and monitoring jobs via API
  ✅ Updating and deleting jobs
  ✅ Reusable templates and automation
  ✅ CI/CD integration patterns

  💡 Best for: Production, automation, version control, scale

🎯 RECOMMENDED WORKFLOW:
  1. Learn with UI (Part 2) - Understand visually
  2. Prototype in UI - Quick experimentation
  3. Move to SDK (Part 3) - Production deployment
  4. Implement CI/CD - Automated deployments
  5. Use both - UI for debugging, SDK for automation

📋 PRODUCTION CHECKLIST:
  ✅ Define clear task dependencies (avoid circular deps)
  ✅ Configure appropriate retries (2-3 for transient errors)
  ✅ Set realistic timeouts (prevent runaway jobs)
  ✅ Use parameterization (flexibility across runs)
  ✅ Implement proper error handling (critical vs optional tasks)
  ✅ Set up monitoring and alerts (email/webhooks)
  ✅ Use cluster pools (faster startup)
  ✅ Tag jobs properly (cost tracking, organization)
  ✅ Version control job definitions (Git)
  ✅ Test in dev before prod deployment

🚀 NEXT STEPS:
  - Notebook 19: Create multi-task job using notebooks 06-09
  - Notebook 20: Python wheel creation tutorial
  - Notebook 21: Production stock market data pipeline with wheel
  - Advanced: Databricks Apps with Streamlit dashboards
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC **Official Documentation:**
# MAGIC - [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/jobs.html)
# MAGIC - [Databricks SDK for Python](https://docs.databricks.com/dev-tools/sdk-python.html)
# MAGIC - [Jobs API Reference](https://docs.databricks.com/api/workspace/jobs)
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - [Production Job Best Practices](https://docs.databricks.com/workflows/jobs/jobs-best-practices.html)
# MAGIC - [Cost Optimization for Jobs](https://docs.databricks.com/administration-guide/cloud-configurations/aws/cost-optimization.html)
# MAGIC
# MAGIC **Community Resources:**
# MAGIC - [Databricks Community Forums](https://community.databricks.com/)
# MAGIC - [GitHub Examples](https://github.com/databricks/databricks-sdk-py/tree/main/examples)