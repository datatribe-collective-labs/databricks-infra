# Week 1: Databricks Fundamentals

Master the Databricks platform, Unity Catalog architecture, and foundational concepts.

## 🎯 Learning Objectives

By the end of Week 1, you will:

- ✅ Understand Databricks platform architecture and workspace navigation
- ✅ Master Unity Catalog three-level namespace (catalog.schema.table)
- ✅ Configure and optimize clusters for different workloads
- ✅ Write efficient Spark code using DataFrames and SQL
- ✅ Leverage Delta Lake for ACID transactions and time travel

## 📚 Notebooks (5)

### 01. Databricks Fundamentals
**Focus:** Platform architecture and core concepts

- Workspace navigation and organization
- Runtime environments and Databricks File System (DBFS)
- Notebook features and magic commands
- Data governance overview

**[Open Notebook →](01-databricks-fundamentals.md)**

---

### 02. Unity Catalog Deep Dive
**Focus:** Data governance and the three-level namespace

- Catalog.Schema.Table architecture
- Metastore concepts and multi-workspace patterns
- Permission models (USE, SELECT, MODIFY, ALL_PRIVILEGES)
- Best practices for data organization

**[Open Notebook →](02-unity-catalog.md)**

---

### 03. Cluster Management
**Focus:** Compute options and optimization

- **All-Purpose Clusters** - Interactive development
- **Job Clusters** - Production workloads (60-94% cost savings)
- **SQL Warehouses** - Analytics and BI
- **Serverless Compute** - Free Edition (zero config)
- **Photon Engine** - 3-10x performance boost
- **Delta Live Tables** - Automated pipeline clusters

**[Open Notebook →](03-cluster-management.md)**

---

### 04. Spark on Databricks
**Focus:** Distributed computing and DataFrame operations

- Spark architecture (driver, executors, tasks)
- DataFrame API vs SQL
- Transformations and actions
- Performance tuning and optimization
- Caching strategies

**[Open Notebook →](04-spark.md)**

---

### 05. Delta Lake Concepts Explained
**Focus:** ACID transactions and lakehouse architecture

- Delta Lake architecture and benefits
- ACID transactions for data lakes
- Time travel and versioning
- OPTIMIZE and Z-ORDER operations
- Schema evolution patterns

**[Open Notebook →](05-delta-lake.md)**

---

## 🚀 Week 1 Project

**Build a complete Unity Catalog structure**

Create a multi-environment data architecture:

1. Set up catalogs for dev/prod environments
2. Create bronze, silver, gold schemas
3. Explore cluster configuration options
4. Write sample data with Delta Lake
5. Query across schemas and catalogs

---

## 💡 Key Concepts

### Three-Level Namespace
```sql
-- Unity Catalog hierarchy
catalog_name.schema_name.table_name

-- Example
sales_prod.gold.daily_revenue
```

### Cluster Types Decision Tree

```
Need compute?
├─ Interactive development? → All-Purpose Cluster
├─ Scheduled ETL job? → Job Cluster (60-94% cheaper!)
├─ SQL/BI analytics? → SQL Warehouse (with Photon)
├─ Free Edition? → Serverless Compute (only option)
└─ Complex ETL pipeline? → Delta Live Tables (automated)
```

### Delta Lake Benefits

- **ACID Transactions**: Reliable concurrent writes
- **Time Travel**: Query historical versions
- **Schema Evolution**: Add/modify columns safely
- **Performance**: Optimized file management
- **Audit Trail**: Complete data lineage

---

## 📊 Skills Matrix

| Skill | Before Week 1 | After Week 1 |
|-------|--------------|--------------|
| Databricks Navigation | ❌ | ✅ |
| Unity Catalog Understanding | ❌ | ✅ |
| Cluster Configuration | ❌ | ✅ |
| Spark DataFrame API | ❌ | ✅ |
| Delta Lake Operations | ❌ | ✅ |

---

## 🎓 Prerequisites

- Basic Python knowledge (variables, functions, loops)
- SQL familiarity helpful but not required
- Databricks workspace access (Free Edition works!)

---

## ⏱️ Time Commitment

**Total:** ~8-10 hours

- Notebooks: 6-8 hours (hands-on)
- Concepts review: 1-2 hours
- Project: 1-2 hours

---

## ✅ Completion Checklist

Track your progress:

- [ ] Complete notebook 01: Databricks Fundamentals
- [ ] Complete notebook 02: Unity Catalog Deep Dive
- [ ] Complete notebook 03: Cluster Management
- [ ] Complete notebook 04: Spark on Databricks
- [ ] Complete notebook 05: Delta Lake Concepts
- [ ] Build Week 1 project (Unity Catalog structure)
- [ ] Understand three-level namespace
- [ ] Know when to use each cluster type

---

## 🔜 Next Steps

After completing Week 1:

**[Continue to Week 2 →](../week2/index.md)** to master data ingestion from files, APIs, databases, and cloud storage.

---

## 📞 Need Help?

- Check troubleshooting sections in each notebook
- Review [Databricks Documentation](https://docs.databricks.com/)
- See [Architecture Guide](../../infrastructure/architecture.md) for infrastructure details