# Unity Catalog Structure

Documentation of the Unity Catalog architecture and schema organization.

## Catalog Architecture

This infrastructure manages 5 Unity Catalogs with distinct purposes.

### Shared Reference Catalogs (Read-only)

**Sales Department Data:**
- `sales_dev` - Development environment
- `sales_prod` - Production environment

**Marketing Department Data:**
- `marketing_dev` - Development environment  
- `marketing_prod` - Production environment

Each contains medallion architecture schemas:
- `bronze` - Raw data
- `silver` - Cleaned data
- `gold` - Business-level aggregations
- `experiments` (marketing_dev only)

### Course Catalog (User Workspaces)

**`databricks_course`** - Single catalog for all student work

Contains:
- 3 shared schemas: `shared_bronze`, `shared_silver`, `shared_gold`
- 8 user-personal schemas (one per user)
- User schemas named from email: `chanukya_pekala`, `komal_azram`, etc.

## Three-Level Namespace

```sql
catalog.schema.table

-- Examples:
sales_dev.bronze.sales_transactions
databricks_course.chanukya_pekala.bronze_sales
marketing_prod.gold.daily_metrics
```

See [Architecture Guide](architecture.md) for complete details.
