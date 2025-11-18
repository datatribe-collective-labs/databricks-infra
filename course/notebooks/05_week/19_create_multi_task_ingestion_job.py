# Databricks notebook source
# MAGIC %md
# MAGIC # Create Multi-Task Ingestion Job - Week 5
# MAGIC
# MAGIC This notebook demonstrates how to create a production-grade multi-task job that orchestrates
# MAGIC data ingestion from multiple sources in parallel.
# MAGIC
# MAGIC ## What We're Building
# MAGIC
# MAGIC A **Bronze Layer Ingestion Pipeline** that processes data from 4 different sources simultaneously:
# MAGIC
# MAGIC ```
# MAGIC Multi-Source Ingestion Job
# MAGIC ├─ Task 1: File Ingestion (CSV, JSON, Parquet)    ─┐
# MAGIC ├─ Task 2: API Ingestion (REST APIs)              ─┤ Parallel
# MAGIC ├─ Task 3: Database Ingestion (JDBC)              ─┤ Execution
# MAGIC └─ Task 4: S3/Cloud Storage Ingestion             ─┘
# MAGIC ```
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC - Create multi-task jobs using **Databricks UI** (visual, beginner-friendly)
# MAGIC - Create multi-task jobs using **Databricks SDK** (programmatic, production-ready)
# MAGIC - Configure parallel task execution
# MAGIC - Run and monitor jobs
# MAGIC - Debug failed tasks
# MAGIC - View ingestion results
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC ✅ Completed Week 2 notebooks (06-09) - These are the notebooks we'll orchestrate
# MAGIC ✅ User schema configured (via `%run ../utils/user_schema_setup`)
# MAGIC ✅ Access to Databricks workspace with job creation permissions

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup

# COMMAND ----------

print("=== Multi-Task Ingestion Job Setup ===\n")
print(f"Catalog: {CATALOG}")
print(f"Schema: {USER_SCHEMA}")
print(f"User: {USER_EMAIL}")
print("\nThis job will create tables in your personal schema:")
print(f"  - {CATALOG}.{USER_SCHEMA}.bronze_* (ingested data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 1: CREATE JOB USING UI (RECOMMENDED FOR LEARNING)
# MAGIC ---
# MAGIC
# MAGIC ## Why Start with UI?
# MAGIC
# MAGIC The UI approach is **more satisfying and intuitive** for learning because:
# MAGIC - ✅ **Visual feedback**: See the DAG (workflow graph) in real-time
# MAGIC - ✅ **Immediate results**: Click, configure, run - no code required
# MAGIC - ✅ **Easy debugging**: Visual task status, logs one click away
# MAGIC - ✅ **Great for prototyping**: Quickly test different configurations
# MAGIC
# MAGIC Once you understand the concepts visually, the SDK approach (Part 2) makes more sense!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-by-Step: Create Job in Databricks UI
# MAGIC
# MAGIC ### Step 1: Navigate to Workflows
# MAGIC
# MAGIC 1. Click **"Workflows"** in the left sidebar
# MAGIC 2. Click **"Create Job"** button (top right, blue button)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Configure Job Name and Settings
# MAGIC
# MAGIC **Job Configuration:**
# MAGIC ```
# MAGIC Name: Bronze_Multi_Source_Ingestion
# MAGIC Description: Parallel ingestion from files, APIs, databases, and cloud storage
# MAGIC Tags:
# MAGIC   - environment: dev
# MAGIC   - layer: bronze
# MAGIC   - pipeline: ingestion
# MAGIC ```
# MAGIC
# MAGIC **Why these settings?**
# MAGIC - **Descriptive name**: Easy to find and understand purpose
# MAGIC - **Tags**: Organize jobs, track costs, filter in UI
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Add Task 1 - File Ingestion
# MAGIC
# MAGIC Click **"+ Add task"** and configure:
# MAGIC
# MAGIC ```
# MAGIC Task name: ingest_files
# MAGIC Type: Notebook
# MAGIC Source: Workspace
# MAGIC Notebook path: /Workspace/course/notebooks/02_week/06_file_ingestion
# MAGIC ```
# MAGIC
# MAGIC **Cluster Configuration:**
# MAGIC - **Option A - Serverless (Recommended):**
# MAGIC   ```
# MAGIC   Compute: Serverless
# MAGIC   ✅ Fastest startup
# MAGIC   ✅ Auto-scaling
# MAGIC   ✅ No cluster management
# MAGIC   ```
# MAGIC
# MAGIC - **Option B - Job Cluster:**
# MAGIC   ```
# MAGIC   Create new job cluster
# MAGIC   Cluster mode: Standard
# MAGIC   Runtime: 14.3 LTS
# MAGIC   Node type: i3.xlarge (adjust based on workspace)
# MAGIC   Workers: Autoscale 2-8
# MAGIC   ```
# MAGIC
# MAGIC **Parameters (Base parameters section):**
# MAGIC ```
# MAGIC catalog: databricks_course
# MAGIC schema: <your_user_schema>  (e.g., chanukya_pekala)
# MAGIC ```
# MAGIC
# MAGIC **Advanced Settings:**
# MAGIC ```
# MAGIC Timeout: 1 hour
# MAGIC Retries: 2
# MAGIC Retry interval: 60 seconds
# MAGIC ```
# MAGIC
# MAGIC Click **"Create task"**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Add Task 2 - API Ingestion (Parallel)
# MAGIC
# MAGIC Click **"+ Add task"** again:
# MAGIC
# MAGIC ```
# MAGIC Task name: ingest_api
# MAGIC Type: Notebook
# MAGIC Source: Workspace
# MAGIC Notebook path: /Workspace/course/notebooks/02_week/07_api_ingest
# MAGIC ```
# MAGIC
# MAGIC **Cluster:**
# MAGIC ```
# MAGIC Compute: Same as Task 1 (recommended for cost efficiency)
# MAGIC ```
# MAGIC
# MAGIC **Depends on:** Leave empty (runs in parallel with Task 1)
# MAGIC
# MAGIC **Parameters:**
# MAGIC ```
# MAGIC catalog: databricks_course
# MAGIC schema: <your_user_schema>
# MAGIC ```
# MAGIC
# MAGIC **Advanced Settings:**
# MAGIC ```
# MAGIC Timeout: 1 hour
# MAGIC Retries: 2
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 5: Add Task 3 - Database Ingestion (Parallel)
# MAGIC
# MAGIC ```
# MAGIC Task name: ingest_database
# MAGIC Type: Notebook
# MAGIC Notebook path: /Workspace/course/notebooks/02_week/08_database_ingest
# MAGIC Compute: Same as previous tasks
# MAGIC Depends on: (empty - parallel execution)
# MAGIC Parameters:
# MAGIC   catalog: databricks_course
# MAGIC   schema: <your_user_schema>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 6: Add Task 4 - S3 Ingestion (Parallel)
# MAGIC
# MAGIC ```
# MAGIC Task name: ingest_s3
# MAGIC Type: Notebook
# MAGIC Notebook path: /Workspace/course/notebooks/02_week/09_s3_ingest
# MAGIC Compute: Same as previous tasks
# MAGIC Depends on: (empty - parallel execution)
# MAGIC Parameters:
# MAGIC   catalog: databricks_course
# MAGIC   schema: <your_user_schema>
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 7: View the DAG (Workflow Graph)
# MAGIC
# MAGIC After adding all 4 tasks:
# MAGIC
# MAGIC 1. Click **"View DAG"** or switch to **"Graph"** view
# MAGIC 2. You should see **4 tasks side-by-side** (no arrows between them = parallel)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
# MAGIC │ ingest_files│  │ ingest_api  │  │ingest_database│ │ ingest_s3  │
# MAGIC └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘
# MAGIC     (Task 1)        (Task 2)         (Task 3)         (Task 4)
# MAGIC
# MAGIC ALL TASKS RUN IN PARALLEL ⚡
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 8: Configure Job-Level Settings (Optional)
# MAGIC
# MAGIC Click on the job name at the top to access job-level settings:
# MAGIC
# MAGIC **Schedule (Optional):**
# MAGIC ```
# MAGIC Schedule type: Cron
# MAGIC Cron expression: 0 2 * * *  (Daily at 2 AM)
# MAGIC Timezone: America/New_York
# MAGIC Pause status: Paused (we'll run manually first)
# MAGIC ```
# MAGIC
# MAGIC **Notifications:**
# MAGIC ```
# MAGIC Email on: Failure, Success
# MAGIC Email to: your-email@example.com
# MAGIC ```
# MAGIC
# MAGIC **Permissions:**
# MAGIC ```
# MAGIC Access control: Can View, Can Run, Can Manage
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 9: Save and Run the Job
# MAGIC
# MAGIC 1. Click **"Create"** or **"Save"** (top right)
# MAGIC 2. Click **"Run now"** to start the job immediately
# MAGIC 3. Watch the execution in real-time!
# MAGIC
# MAGIC **What happens:**
# MAGIC - All 4 tasks start simultaneously
# MAGIC - Each task shows progress (Running → Success/Failed)
# MAGIC - Total execution time ≈ slowest task (not sum of all tasks!)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 10: Monitor Execution
# MAGIC
# MAGIC **While job is running:**
# MAGIC - See task status in real-time
# MAGIC - Click any task to view logs
# MAGIC - Spark UI link available for performance analysis
# MAGIC
# MAGIC **After completion:**
# MAGIC - Green checkmarks ✅ = Success
# MAGIC - Red X ❌ = Failed (click to see error)
# MAGIC - View output of each task
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 11: View Ingested Data
# MAGIC
# MAGIC After job completes successfully, verify data:
# MAGIC
# MAGIC 1. Go to **"Data"** in left sidebar
# MAGIC 2. Navigate to `databricks_course` → `<your_schema>`
# MAGIC 3. Look for new Bronze tables created by each task

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Common UI Issues
# MAGIC
# MAGIC ### Issue 1: Task Fails Immediately
# MAGIC
# MAGIC **Possible causes:**
# MAGIC - Notebook path incorrect
# MAGIC - Missing parameters
# MAGIC - Permissions issues
# MAGIC
# MAGIC **Solution:**
# MAGIC 1. Click failed task → "Output" tab
# MAGIC 2. Read error message
# MAGIC 3. Fix issue and click "Repair run"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 2: Cluster Takes Long to Start
# MAGIC
# MAGIC **Solution:**
# MAGIC - Switch to **Serverless** compute (instant start)
# MAGIC - OR use **Cluster Pools** for faster startup
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 3: Can't Find Job After Creation
# MAGIC
# MAGIC **Solution:**
# MAGIC - Workflows → "All jobs" tab
# MAGIC - Use search bar (top right)
# MAGIC - Filter by tags

# COMMAND ----------

print("=== UI Job Creation Summary ===\n")

print("""
✅ WHAT YOU ACCOMPLISHED:
  1. Created a multi-task job in Databricks UI
  2. Configured 4 parallel ingestion tasks
  3. Set up parameters for Unity Catalog
  4. Configured retries and timeouts
  5. Ran the job and monitored execution
  6. Viewed ingestion results

🎓 KEY LEARNINGS:
  - Visual DAG shows task dependencies
  - Parallel tasks = faster execution
  - Each task can have different configs
  - Retry policies prevent transient failures
  - UI makes debugging easy

➡️ NEXT: Learn to do the same thing programmatically with SDK!
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART 2: CREATE JOB USING DATABRICKS SDK (PRODUCTION-READY)
# MAGIC ---
# MAGIC
# MAGIC ## Why Use SDK?
# MAGIC
# MAGIC After mastering the UI, the SDK approach offers:
# MAGIC - ✅ **Automation**: Create jobs from code/scripts
# MAGIC - ✅ **Version Control**: Job definitions in Git
# MAGIC - ✅ **Reproducibility**: Same job across dev/staging/prod
# MAGIC - ✅ **CI/CD Integration**: Automated deployments
# MAGIC - ✅ **Scale**: Manage 100s of jobs programmatically

# COMMAND ----------

# Install Databricks SDK
%pip install databricks-sdk --quiet

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import Task, NotebookTask, Source
from databricks.sdk.service.compute import AutoScale

# Initialize SDK client (auto-authenticated in Databricks notebooks)
w = WorkspaceClient()

print("=== Databricks SDK Initialized ===")
print(f"✅ Connected to: {w.config.host}")
print(f"✅ Current user: {w.current_user.me().user_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Job Configuration

# COMMAND ----------

print("=== Defining Multi-Task Ingestion Job ===\n")

# Job name
job_name = f"SDK_Bronze_Ingestion_{USER_SCHEMA}"

# Base parameters to pass to all notebooks
base_params = {
    "catalog": CATALOG,
    "schema": USER_SCHEMA
}

# Define all 4 ingestion tasks
tasks = [
    Task(
        task_key="ingest_files",
        description="Ingest CSV, JSON, and Parquet files to Bronze layer",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/06_file_ingestion",
            source=Source.WORKSPACE,
            base_parameters=base_params

        ),
        timeout_seconds=3600,  # 1 hour
        max_retries=2,
        min_retry_interval_millis=60000  # 1 minute between retries
    ),
    Task(
        task_key="ingest_api",
        description="Ingest data from REST APIs to Bronze layer",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/07_api_ingest",
            source=Source.WORKSPACE,
            base_parameters=base_params
        ),
        timeout_seconds=3600,
        max_retries=2,
        min_retry_interval_millis=60000
    ),
    Task(
        task_key="ingest_database",
        description="Ingest data from databases via JDBC to Bronze layer",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/08_database_ingest",
            source=Source.WORKSPACE,
            base_parameters=base_params
        ),
        timeout_seconds=3600,
        max_retries=2,
        min_retry_interval_millis=60000
    ),
    Task(
        task_key="ingest_s3",
        description="Ingest data from S3/cloud storage to Bronze layer",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Shared/terraform-managed/course/notebooks/02_week/09_s3_ingest",
            source=Source.WORKSPACE,
            base_parameters=base_params
        ),
        timeout_seconds=3600,
        max_retries=2,
        min_retry_interval_millis=60000
    )
]

print(f"Job Name: {job_name}")
print(f"Total Tasks: {len(tasks)}")
print(f"\nTask Configuration:")
for i, task in enumerate(tasks, 1):
    print(f"  {i}. {task.task_key}")
    print(f"     Description: {task.description}")
    print(f"     Notebook: {task.notebook_task.notebook_path}")
    print(f"     Timeout: {task.timeout_seconds}s")
    print(f"     Max Retries: {task.max_retries}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Job

# COMMAND ----------

print("=== Creating Job via SDK ===\n")

try:
    # Check if job already exists (to avoid duplicates)
    existing_jobs = list(w.jobs.list(name=job_name))

    if existing_jobs:
        # Job exists, update it
        job_id = existing_jobs[0].job_id
        print(f"⚠️  Job already exists (ID: {job_id})")
        print(f"   Updating existing job configuration...")

        w.jobs.update(
            job_id=job_id,
            new_settings=jobs.JobSettings(
                name=job_name,
                tasks=tasks,
                tags={
                    "environment": "dev",
                    "layer": "bronze",
                    "pipeline": "ingestion",
                    "managed_by": "sdk",
                    "user": USER_SCHEMA
                },
                max_concurrent_runs=1,
                timeout_seconds=7200  # 2 hour job-level timeout
            )
        )

        print(f"✅ Job updated successfully!")

    else:
        # Create new job
        print(f"Creating new job: {job_name}")

        created_job = w.jobs.create(
            name=job_name,
            tasks=tasks,
            tags={
                "environment": "dev",
                "layer": "bronze",
                "pipeline": "ingestion",
                "managed_by": "sdk",
                "user": USER_SCHEMA
            },
            max_concurrent_runs=1,
            timeout_seconds=7200
        )

        job_id = created_job.job_id
        print(f"✅ Job created successfully!")

    print(f"\nJob Details:")
    print(f"  Job ID: {job_id}")
    print(f"  Job Name: {job_name}")
    print(f"  Tasks: {len(tasks)} parallel ingestion tasks")
    print(f"  View in UI: {w.config.host}/#job/{job_id}")

    # Store for later use
    dbutils.jobs.taskValues.set(key="job_id", value=str(job_id))

except Exception as e:
    print(f"❌ Error creating/updating job: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Job

# COMMAND ----------

print("=== Running Multi-Task Ingestion Job ===\n")

try:
    # Retrieve job ID
    job_id = int(dbutils.jobs.taskValues.get(taskKey="", key="job_id", default="0", debugValue="0"))

    if job_id == 0:
        print("⚠️  Job ID not found. Please run the 'Create the Job' cell first.")
    else:
        print(f"Triggering job run for Job ID: {job_id}")
        print(f"Job name: {job_name}\n")

        # Trigger job run
        run = w.jobs.run_now(job_id=job_id)

        run_id = run.run_id

        print(f"✅ Job run triggered successfully!")
        print(f"\nRun Details:")
        print(f"  Run ID: {run_id}")
        print(f"  Job ID: {job_id}")
        print(f"  View run: {w.config.host}/#job/{job_id}/run/{run_id}")

        print(f"\n📊 Monitoring execution...")
        print(f"   All 4 ingestion tasks will run in parallel")
        print(f"   Expected duration: ~2-5 minutes (depends on data volume)")

        # Store run ID for monitoring
        dbutils.jobs.taskValues.set(key="run_id", value=str(run_id))

except Exception as e:
    print(f"❌ Error running job: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Job Execution

# COMMAND ----------

import time
from datetime import datetime

print("=== Real-Time Job Monitoring ===\n")

try:
    # Retrieve run ID
    run_id = int(dbutils.jobs.taskValues.get(taskKey="", key="run_id", default="0", debugValue="0"))

    if run_id == 0:
        print("⚠️  Run ID not found. Please run the 'Run the Job' cell first.")
    else:
        print(f"Monitoring Run ID: {run_id}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print("-" * 80)

        # Monitor until completion or timeout
        max_wait_seconds = 600  # 10 minutes max
        check_interval = 10  # Check every 10 seconds
        elapsed = 0

        while elapsed < max_wait_seconds:
            # Get current run status
            run_status = w.jobs.get_run(run_id=run_id)

            # Overall job state
            lifecycle_state = run_status.state.life_cycle_state
            result_state = run_status.state.result_state

            print(f"[{elapsed}s] Job State: {lifecycle_state}", end="")
            if result_state:
                print(f" | Result: {result_state}")
            else:
                print()

            # Task-level status
            if run_status.tasks:
                print("  Task Status:")
                for task_run in run_status.tasks:
                    task_state = task_run.state.life_cycle_state
                    task_result = task_run.state.result_state or "PENDING"

                    # Status emoji
                    if task_state == "RUNNING":
                        emoji = "🔄"
                    elif task_result == "SUCCESS":
                        emoji = "✅"
                    elif task_result == "FAILED":
                        emoji = "❌"
                    else:
                        emoji = "⏳"

                    print(f"    {emoji} {task_run.task_key:<20} | {task_state:<12} | {task_result}")

            # Check if job completed
            if lifecycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                print("\n" + "=" * 80)
                print(f"🏁 Job completed!")
                print(f"   Final Result: {result_state}")
                print(f"   Total Duration: {elapsed} seconds")

                # Show final task results
                if run_status.tasks:
                    print(f"\n📋 Task Summary:")
                    success_count = 0
                    failed_count = 0

                    for task_run in run_status.tasks:
                        task_result = task_run.state.result_state
                        if task_result == "SUCCESS":
                            success_count += 1
                            print(f"   ✅ {task_run.task_key} - SUCCESS")
                        else:
                            failed_count += 1
                            print(f"   ❌ {task_run.task_key} - {task_result}")

                    print(f"\n   Total: {len(run_status.tasks)} tasks")
                    print(f"   Success: {success_count}")
                    print(f"   Failed: {failed_count}")

                break

            # Wait before next check
            time.sleep(check_interval)
            elapsed += check_interval
            print()

        if elapsed >= max_wait_seconds:
            print(f"\n⚠️  Monitoring timeout after {max_wait_seconds} seconds")
            print(f"   Job is still running. Check UI for latest status:")
            print(f"   {w.config.host}/#job/{job_id}/run/{run_id}")

except Exception as e:
    print(f"❌ Error monitoring job: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Ingestion Results

# COMMAND ----------

print("=== Viewing Ingestion Results ===\n")

print(f"Checking tables created in: {CATALOG}.{USER_SCHEMA}\n")

# List all bronze tables
try:
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{USER_SCHEMA}").collect()

    bronze_tables = [row.tableName for row in tables if row.tableName.startswith('bronze_')]

    if bronze_tables:
        print(f"✅ Found {len(bronze_tables)} Bronze tables:\n")

        for table_name in sorted(bronze_tables):
            full_table = f"{CATALOG}.{USER_SCHEMA}.{table_name}"

            # Get row count
            try:
                count = spark.table(full_table).count()
                print(f"  📊 {table_name}")
                print(f"      Records: {count:,}")

                # Show sample data
                print(f"      Sample:")
                spark.table(full_table).show(3, truncate=True, vertical=False)
                print()

            except Exception as e:
                print(f"  ⚠️  {table_name} - Error reading: {e}")
    else:
        print("⚠️  No Bronze tables found yet.")
        print("   This could mean:")
        print("   - Job is still running")
        print("   - Job failed (check logs)")
        print("   - Notebooks didn't create tables (check notebook logic)")

except Exception as e:
    print(f"❌ Error querying tables: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Management Operations

# COMMAND ----------

print("=== Job Management Operations ===\n")

try:
    job_id = int(dbutils.jobs.taskValues.get(taskKey="", key="job_id", default="0", debugValue="0"))

    if job_id > 0:
        # Get job details
        job = w.jobs.get(job_id=job_id)

        print(f"Job Information:")
        print(f"  ID: {job.job_id}")
        print(f"  Name: {job.settings.name}")
        print(f"  Created by: {job.creator_user_name}")
        print(f"  Tasks: {len(job.settings.tasks)}")
        print(f"  Tags: {job.settings.tags}")

        # List recent runs
        print(f"\n📜 Recent Runs:")
        runs = w.jobs.list_runs(job_id=job_id, limit=5)

        for run in runs:
            run_state = run.state.result_state or run.state.life_cycle_state
            emoji = "✅" if run_state == "SUCCESS" else ("❌" if run_state == "FAILED" else "🔄")

            # Format start time
            start_time = datetime.fromtimestamp(run.start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')

            print(f"  {emoji} Run {run.run_id}")
            print(f"     Started: {start_time}")
            print(f"     State: {run_state}")
            print()

except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC **⚠️ WARNING:** Uncommenting and running this will delete the job!

# COMMAND ----------

print("=== Job Cleanup ===\n")

# Uncomment to delete the job
# try:
#     job_id = int(dbutils.jobs.taskValues.get(taskKey="", key="job_id", default="0", debugValue="0"))
#
#     if job_id > 0:
#         w.jobs.delete(job_id=job_id)
#         print(f"✅ Deleted job: {job_id}")
#     else:
#         print("⚠️  No job ID found to delete")
#
# except Exception as e:
#     print(f"❌ Error deleting job: {e}")

print("⚠️  Job deletion is commented out to prevent accidental deletion")
print("   Uncomment the code above if you want to delete the job")
print(f"\n💡 To delete manually: Go to Workflows → Find job → ⋮ menu → Delete")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary and Key Takeaways
# MAGIC ---

# COMMAND ----------

print("=== Multi-Task Ingestion Job - Complete! ===\n")

print("""
🎓 WHAT YOU LEARNED:

1️⃣ UI APPROACH (Part 1):
   ✅ Create multi-task jobs visually
   ✅ Configure parallel task execution
   ✅ Set up parameters and retries
   ✅ Monitor execution in real-time
   ✅ Debug failed tasks with logs

   💡 Best for: Learning, prototyping, visual debugging

2️⃣ SDK APPROACH (Part 2):
   ✅ Create jobs programmatically with Python
   ✅ Define task configurations in code
   ✅ Trigger jobs via API
   ✅ Monitor execution programmatically
   ✅ Manage jobs (update, delete, list runs)

   💡 Best for: Production, automation, CI/CD, version control

🚀 WHAT YOU BUILT:

   A production-grade Bronze layer ingestion pipeline that:
   - Ingests from 4 different sources simultaneously
   - Runs in parallel (4x faster than sequential)
   - Has retry logic for resilience
   - Uses Unity Catalog for data isolation
   - Can be scheduled or triggered on-demand

📊 INGESTION SOURCES:
   ✅ Files (CSV, JSON, Parquet) - Notebook 06
   ✅ REST APIs - Notebook 07
   ✅ Databases (JDBC) - Notebook 08
   ✅ Cloud Storage (S3) - Notebook 09

⚡ PERFORMANCE:
   Sequential execution: ~20 minutes (4 tasks × 5 min each)
   Parallel execution: ~5 minutes (max of all tasks)
   Speedup: 4x faster! ⚡

🎯 NEXT STEPS:

   ✅ Notebook 20: Learn to build Python wheels
   ✅ Notebook 21: Deploy stock market pipeline with wheel
   ✅ Advanced: Build Databricks Apps with Streamlit

💡 PRO TIPS:

   1. Start with UI to learn visually
   2. Move to SDK for production automation
   3. Use tags to organize jobs
   4. Set up email alerts for failures
   5. Enable retries for transient errors
   6. Use serverless compute for faster starts
   7. Monitor job history to optimize performance
   8. Version control your SDK job definitions (Git)

📚 ADDITIONAL RESOURCES:
   - Databricks Jobs Docs: https://docs.databricks.com/workflows/jobs/jobs.html
   - SDK Reference: https://docs.databricks.com/dev-tools/sdk-python.html
   - Best Practices: https://docs.databricks.com/workflows/jobs/jobs-best-practices.html
""")

print("\n🎉 Congratulations! You've mastered multi-task job orchestration!")