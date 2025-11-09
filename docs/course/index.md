# Course Overview

A comprehensive 5-week Databricks data engineering course with an advanced section, totaling **19 hands-on notebooks** that take you from platform fundamentals to production deployment.

## 📚 Course Structure (5 Weeks + Advanced, 19 Notebooks)

| Week | Focus | Notebooks | Duration | Key Skills |
|------|-------|-----------|----------|------------|
| **[Week 1](week1/index.md)** | Databricks Fundamentals | 5 | 1 week | Platform mastery, Unity Catalog, cluster management |
| **[Week 2](week2/index.md)** | Data Ingestion | 5 | 1 week | Files, APIs, databases, cloud storage patterns |
| **[Week 3](week3/index.md)** | Data Transformations | 4 | 1 week | Advanced Spark operations, window functions |
| **[Week 4](week4/index.md)** | End-to-End Workflows | 3 | 1 week | Complete pipeline development |
| **[Week 5](week5/index.md)** | Production Deployment | 4 | 1-2 weeks | Job orchestration, wheel packages, production patterns |
| **[Advanced](advanced/index.md)** | Databricks Apps | 2 | Optional | Interactive data applications with Streamlit |

**Total:** 19 notebooks • 4-6 weeks • Beginner to Production-Ready

---

## 🎯 Learning Paths

### 🟢 New to Databricks (3-4 weeks)

Perfect if you're new to the Databricks platform or Spark.

**Focus:** Weeks 1-4

- **Week 1**: Understand the platform, Unity Catalog, and cluster concepts
- **Week 2**: Master data ingestion patterns from various sources
- **Week 3**: Learn transformations and analytics operations
- **Week 4**: Build your first complete end-to-end pipeline

### 🟡 Know Spark/Data Engineering (2-3 weeks)

You understand Spark but want to master Databricks-specific features.

**Focus:** Weeks 3-5

- **Week 3**: Advanced transformations and window functions
- **Week 4**: Production-grade pipeline patterns
- **Week 5**: Job orchestration and wheel deployment

### 🔴 Production Ready (1-2 weeks)

You're experienced and want production deployment patterns.

**Focus:** Week 5 + Advanced

- **Week 5**: Job orchestration, Poetry packaging, production deployment
- **Advanced**: Build interactive Streamlit applications

### ⭐ Complete Journey (4-6 weeks)

Comprehensive path from fundamentals to production applications.

**Follow sequentially:** Weeks 1 → 2 → 3 → 4 → 5 → Advanced

1. Databricks fundamentals and platform features
2. Data engineering patterns (ingestion, transformations, pipelines)
3. Production deployment with professional Python packaging
4. Interactive applications for stakeholders (no-code UX)

---

## 📖 What You'll Learn

### Week 1: Foundation & Platform Mastery

Learn the Databricks platform from the ground up.

- Platform architecture and workspace navigation
- Unity Catalog three-level namespace (catalog.schema.table)
- Cluster types, sizing, and cost optimization
- Spark fundamentals and performance tuning
- Delta Lake ACID transactions and time travel

**→ [Start Week 1](week1/index.md)**

---

### Week 2: Data Ingestion Mastery

Master production-grade data ingestion from multiple sources.

- File ingestion (CSV, JSON, Parquet) with schema enforcement
- REST API integration with authentication and retry logic
- JDBC database connections and incremental loading
- S3 cloud storage and data lakehouse patterns
- Error handling and data quality validation

**→ [Start Week 2](week2/index.md)**

---

### Week 3: Advanced Transformations

Build complex data transformations and analytics.

- Data cleaning and type conversions
- Window functions for ranking and analytics
- Complex aggregations with CUBE and ROLLUP
- Performance optimization techniques
- Real-world transformation patterns

**→ [Start Week 3](week3/index.md)**

---

### Week 4: End-to-End Workflows

Combine everything into complete data pipelines.

- Bronze → Silver → Gold medallion architecture
- File processing pipelines with monitoring
- API to insights workflows
- Production error handling patterns
- Pipeline orchestration basics

**→ [Start Week 4](week4/index.md)**

---

### Week 5: Production Deployment

Deploy professional, production-ready data applications.

- Databricks Jobs API (UI + SDK approaches)
- Multi-task job orchestration
- Python wheel creation with Poetry
- Real production example: Stock market data pipeline
- CI/CD integration patterns

**→ [Start Week 5](week5/index.md)**

---

### Advanced: Databricks Apps

Build interactive data applications for non-technical users.

- Streamlit framework on Databricks
- Interactive dashboards from gold tables
- Real-time stock market analyzer application
- App deployment and sharing
- Production app patterns

**→ [Start Advanced](advanced/index.md)**

---

## 🎓 Prerequisites

### Minimum Requirements

- **Python knowledge**: Basic Python syntax (variables, functions, loops)
- **SQL familiarity**: SELECT, JOIN, GROUP BY (helpful but not required)
- **Databricks account**: Free tier or provided workspace access

### Recommended Skills

- Basic understanding of data concepts (tables, schemas, databases)
- Command line familiarity (helpful for Platform Engineers)
- Git basics (for version control of notebooks)

### No Experience Needed

- ❌ Spark - we teach from scratch
- ❌ Databricks platform - complete introduction provided
- ❌ Unity Catalog - covered comprehensively
- ❌ Infrastructure/Terraform - optional for Data Engineers

---

## 🛠️ Development Environment

### For Data Engineers

**Zero setup required!** Everything runs in Databricks workspace:

- Serverless compute (Free Edition compatible)
- Pre-loaded sample datasets
- Interactive notebooks with immediate execution
- All dependencies pre-installed

### For Platform Engineers

**Local development tools:**

- Terraform (~> 1.0)
- Databricks CLI
- Poetry (Python dependency management)
- Git

---

## 📊 Project-Based Learning

Every week includes hands-on projects:

- **Week 1**: Explore real Unity Catalog structure
- **Week 2**: Ingest multi-source data (files, APIs, databases, S3)
- **Week 3**: Build analytics with window functions
- **Week 4**: Create complete Bronze→Silver→Gold pipeline
- **Week 5**: Deploy production wheel package with orchestration
- **Advanced**: Build interactive stock market analyzer app

---

## 🎯 Learning Outcomes

After completing this course, you will:

✅ **Understand** Databricks platform architecture and Unity Catalog
✅ **Master** data ingestion from multiple sources with error handling
✅ **Build** complex transformations using Spark SQL and DataFrames
✅ **Create** end-to-end medallion architecture pipelines
✅ **Deploy** production-ready applications with wheel packages
✅ **Orchestrate** multi-task workflows with Databricks Jobs
✅ **Develop** interactive applications using Databricks Apps

---

## 💡 Tips for Success

1. **Start with Week 1** even if you know Spark - Databricks has unique platform features
2. **Clone before editing** - copy notebooks to your personal folder
3. **Experiment freely** - modify code and try different parameters
4. **Create your own data** to test different scenarios
5. **Follow cluster recommendations** in notebooks for optimal performance
6. **Use user schema isolation** - notebooks automatically isolate your data

---

## 📞 Need Help?

- **Notebook issues**: Check troubleshooting sections within each notebook
- **Databricks questions**: [Official Databricks Documentation](https://docs.databricks.com/)
- **Course issues**: GitHub Issues or contact instructor
- **Technical setup**: See [Platform Engineer Guide](../getting-started/platform-engineer.md)

---

Ready to start learning? Jump into [Week 1 →](week1/index.md)