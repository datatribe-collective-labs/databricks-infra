# Data Engineer Guide - Zero-Setup Learning

*"I want to learn Databricks through hands-on notebooks and work with data"*

## 🚀 Get Started in 3 Steps

### Step 1: Access Databricks Workspace
**Option A: Use Provided Workspace**
- Get workspace URL from instructor
- Login with provided credentials
- **Zero setup required**

**Option B: Your Own Free Trial**
- Sign up at [databricks.com/try-databricks](https://databricks.com/try-databricks)
- Complete workspace setup (2 minutes)
- No credit card required

### Step 2: Find the Course Content
```bash
# In your Databricks workspace:
# 1. Navigate to: Workspace → Shared → terraform-managed → course → notebooks
# 2. All course content is already there!
# 3. Start with: 01_week/00_databricks_fundamentals.py
```

### Step 3: Start Learning
```bash
# Copy notebooks to your personal space to edit:
# 1. Right-click any notebook → Clone
# 2. Save to: /Users/{your-email}/my-learning/
# 3. Edit, experiment, and learn!
```

## 📚 Course Structure (5 Weeks, 16 Notebooks)

| Week | Focus | Notebooks | What You'll Learn |
|------|-------|-----------|-------------------|
| **1** | Databricks Fundamentals | 4 | Platform mastery, Unity Catalog, cluster management |
| **2** | Data Ingestion | 4 | Files, APIs, databases, cloud storage patterns |  
| **3** | Data Transformations | 3 | Advanced Spark operations, window functions |
| **4** | End-to-End Workflows | 2 | Complete pipeline development |
| **5** | Job Orchestration | 3 | Production automation and monitoring |

## 🎯 Learning Paths

### 🟢 New to Databricks (2-3 weeks)
- Week 1: Complete all notebooks to understand platform
- Week 2: Focus on file ingestion with sample datasets

### 🟡 Know Spark/Data Engineering (2 weeks)  
- Week 3: Master complex transformations and analytics
- Week 4: Build complete end-to-end pipelines

### 🔴 Production Ready (1 week)
- Week 5: Job orchestration and workflow automation

## 🗂️ Course Content Details

### Week 1: Foundation & Platform Mastery
- `00_databricks_fundamentals.py` - Platform architecture and best practices
- `01_unity_catalog_deep_dive.py` - Data governance and permissions  
- `02_cluster_management.py` - Compute optimization and cost management
- `03_spark_on_databricks.py` - Distributed computing and performance

### Week 2: Data Ingestion Mastery  
- `04_file_ingestion.py` - CSV, JSON, Parquet with Delta Lake integration
- `05_api_ingest.py` - REST APIs, authentication, real-time streaming
- `06_database_ingest.py` - JDBC connections and database integration
- `07_s3_ingest.py` - Cloud storage patterns and data lakehouse

### Week 3: Advanced Transformations
- `08_simple_transformations.py` - Data cleaning and basic operations
- `09_window_transformations.py` - Advanced analytics with window functions  
- `10_aggregations.py` - Complex grouping and statistical operations

### Week 4: Production Workflows
- `11_file_to_aggregation.py` - Complete file processing pipeline
- `12_api_to_aggregation.py` - Real-time API data to insights

### Week 5: Automation & Orchestration
- `13_create_job_with_notebook.py` - Scheduled notebook execution
- `14_create_job_with_wheel.py` - Python package deployment
- `15_orchestrate_tasks_in_job.py` - Multi-task workflow orchestration

## 📊 Working with Sample Data

### Access Sample Datasets
```bash
# Datasets are available at:
# /Shared/terraform-managed/course/datasets/
# 
# Include:
# - customers.csv (sample customer data)
# - transactions.json (sample transaction data)  
# - products.parquet (sample product catalog)
```

### Create Your Own Catalogs
```bash
# When notebooks prompt for catalogs:
# 1. Data → Create Catalog
# 2. Name it: "my_learning_catalog"
# 3. Use default settings
# 4. Update notebook catalog references
```

## 🔄 Getting Updates

### When New Content is Added
```bash
# New notebooks automatically appear in the shared folder
# 1. Check: /Shared/terraform-managed/course/notebooks/
# 2. Look for new week folders or updated notebooks
# 3. Clone new content to your personal space
# 4. Continue learning!
```

## 💡 Tips for Success

- **Start with Week 1** even if you know Spark - Databricks has unique features
- **Clone before editing** - copy notebooks to your personal folder
- **Experiment freely** - modify code, try different parameters
- **Create your own data** to test different scenarios
- **Use cluster recommendations** in notebooks for optimal performance

## ❓ Need Help?

- **Notebook issues**: Check troubleshooting sections within each notebook
- **Databricks questions**: Refer to [Databricks documentation](https://docs.databricks.com/)
- **Course issues**: Contact your instructor or use course discussion forum

## 🚀 Advanced: Optional Local Development

*Only if you want to work with git and sync notebooks locally*

```bash
# 1. Clone repository
git clone <repo-url> && cd databricks-infra

# 2. Install Databricks CLI  
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks auth login

# 3. Sync notebooks to your personal workspace
databricks workspace import-dir course/notebooks /Users/{your-email}/local-course --overwrite
```

**Ready to start your Databricks journey? Head to the shared workspace and begin with Week 1!** 🚀