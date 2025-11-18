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
- **Choose your edition:**
  - **Free Edition**: Serverless compute, free forever, no credit card required
    - This repository is designed for serverless compute (notebooks work out-of-the-box)
  - **Classic Trial**: 14-day trial with $200 credits, full cluster management features
    - Provides deeper hands-on experience with cluster configuration
- Complete workspace setup (2 minutes)
- **Note**: For in-depth understanding of clusters and warehouses, see Week 1 notebooks

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

## 📚 Course Structure (5 Weeks + Foundations + Advanced, 27 Notebooks)

| Module | Focus | Notebooks | What You'll Learn |
|--------|-------|-----------|-------------------|
| **Week 1** | Databricks Fundamentals | 5 | Platform mastery, Unity Catalog, cluster management, Spark, Delta Lake |
| **Foundations** | Data Modelling Patterns | 4 | Data modeling basics, medallion architecture, dimensional modeling, SCDs |
| **Week 2** | Data Ingestion | 5 | Files, APIs, databases, cloud storage patterns |
| **Week 3** | Data Transformations | 4 | Advanced Spark operations, window functions |
| **Week 4** | End-to-End Workflows | 3 | Complete pipeline development |
| **Week 5** | Production Deployment | 4 | Job orchestration, wheel packages, production patterns |
| **Advanced** | Databricks Apps | 2 | Interactive data applications with Streamlit |

## 🎯 Learning Paths

### 🟢 New to Databricks (3-4 weeks)
- **Week 1**: Master Databricks platform fundamentals
- **Foundations**: Learn essential data modeling concepts
- **Weeks 2-3**: Complete data ingestion and transformations
- **Week 4**: Build complete end-to-end pipelines

### 🟡 Know Spark/Data Engineering (2-3 weeks)
- **Foundations**: Review data modeling patterns and medallion architecture
- **Weeks 3-4**: Master complex transformations and complete pipelines
- **Week 5**: Production deployment with wheels and job orchestration

### 🔴 Production Ready (1-2 weeks)
- **Week 5**: Job orchestration, wheel packages, and production patterns
- **Advanced**: Build interactive data applications with Streamlit

### ⭐ Complete Journey (4-6 weeks)
Follow all modules sequentially for comprehensive mastery:
1. Databricks fundamentals and platform features
2. Data modeling foundations for maintainable architectures
3. Data engineering patterns (ingestion, transformations, pipelines)
4. Production deployment with professional Python packaging
5. Interactive applications for stakeholders (no-code UX)

## 🗂️ Course Content Details

### Week 1: Databricks Fundamentals
- `01_databricks_fundamentals.py` - Platform architecture and best practices
- `02_unity_catalog_deep_dive.py` - Data governance and permissions
- `03_cluster_management.py` - Compute optimization and cost management
- `04_spark_on_databricks.py` - Distributed computing and performance
- `05_delta_lake_concepts_explained.py` - Delta Lake fundamentals and ACID properties

### Foundations: Data Modelling Patterns
- `01_introduction_to_data_modeling.py` - Data modeling fundamentals and design principles
- `02_medallion_architecture.py` - Bronze, Silver, Gold layers and data lakehouse patterns
- `03_dimensional_modeling.py` - Star schema, facts, dimensions, and data warehouse design
- `04_scd_and_delta_patterns.py` - Slowly Changing Dimensions and Delta Lake implementation

### Week 2: Data Ingestion Mastery
- `06_file_ingestion.py` - CSV, JSON, Parquet with Delta Lake integration
- `07_api_ingest.py` - REST APIs, authentication, retry logic
- `08_database_ingest.py` - JDBC connections and database integration
- `09_s3_ingest.py` - Cloud storage patterns and data lakehouse
- `10_ingestion_concepts_explained.py` - Batch/streaming ingestion and schema handling

### Week 3: Advanced Transformations
- `11_simple_transformations.py` - Data cleaning and basic operations
- `12_window_transformations.py` - Advanced analytics with window functions
- `13_aggregations.py` - Complex grouping and statistical operations
- `14_transformation_concepts_explained.py` - Lazy evaluation, partitioning, caching strategies

### Week 4: End-to-End Workflows
- `15_file_to_aggregation.py` - Complete file processing pipeline
- `16_api_to_aggregation.py` - Real-time API data to insights
- `17_pipeline_patterns_explained.py` - Medallion architecture, data quality, monitoring patterns

### Week 5: Production Deployment
- `18_job_orchestration_concepts_explained.py` - Orchestration fundamentals (UI + SDK)
- `19_create_multi_task_ingestion_job.py` - Multi-task job with real-time monitoring
- `20_wheel_creation_with_poetry.py` - Professional Python packaging with Poetry
- `21_stock_market_wheel_deployment.py` - Production pipeline with real stock market data

### Advanced: Databricks Apps
- `01_databricks_apps_guide.py` - Complete guide to building data applications
- `02_stock_market_analyzer_app.py` - Interactive Streamlit app using gold layer data

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