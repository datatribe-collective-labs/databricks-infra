# End-to-End Data Engineering Framework

> A comprehensive Python package for building production-grade data pipelines on Databricks

## Overview

The `end2end` framework consolidates all patterns taught in the Databricks course (Weeks 1-5) into a reusable, production-ready package. It provides a standardized approach to building data pipelines with built-in best practices for ingestion, transformation, quality validation, and orchestration.

## Why Use end2end?

| Benefit | Description |
|---------|-------------|
| **Reusability** | Write once, use across all pipelines |
| **Consistency** | Standardized patterns throughout your projects |
| **Maintainability** | Centralized logic, easier updates |
| **Production-Ready** | Built-in error handling, logging, retry logic |
| **Educational** | Well-documented code demonstrating best practices |

## Framework Architecture

```
end2end/
├── config.py              # Configuration management
├── ingestion/             # Data ingestion
│   ├── base.py            # Base ingestion class
│   ├── file_ingestion.py  # File sources (CSV, JSON, Parquet)
│   ├── api_ingestion.py   # REST API sources
│   └── database_ingestion.py  # Database sources (JDBC)
├── transformations/       # Medallion architecture
│   ├── base.py            # Base transformation class
│   ├── bronze_to_silver.py  # Data cleaning
│   └── silver_to_gold.py    # Analytics aggregations
├── quality/               # Data quality
│   ├── validators.py      # Schema and data validation
│   └── expectations.py    # Expectation-based testing
├── pipeline/              # Orchestration
│   ├── task.py            # Task abstraction
│   └── pipeline.py        # Pipeline orchestration
└── utils/                 # Utilities
    ├── logging.py         # Structured logging
    └── spark_utils.py     # Spark helpers
```

## Quick Start

### Installation

**For Production (Premium Edition):**

```bash
# Build wheel package
cd /path/to/databricks-infra-org
poetry build

# Upload to Unity Catalog Volumes
dbutils.fs.cp(
    "file:/path/to/dist/databricks_infra-0.1.0-py3-none-any.whl",
    "dbfs:/Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl"
)

# Install in notebook
%pip install /Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl
```

**For Development (Free Edition):**

```python
# In Databricks notebook
import sys
import os

course_root = os.path.abspath("../../..")
src_path = os.path.join(course_root, "src")
sys.path.insert(0, src_path)
```

### Basic Usage

```python
from end2end import (
    PipelineConfig,
    FileIngestion,
    BronzeToSilver,
    SilverToGold,
    Pipeline,
    Task,
)

# 1. Configure pipeline
config = PipelineConfig(
    catalog="databricks_course",
    source_schema="your_schema",
    environment="dev"
)

# 2. Define tasks
def ingest():
    ingestion = FileIngestion(
        config=config,
        source_path="/data/sales.csv",
        file_format="csv",
    )
    return ingestion.execute("sales")

def transform():
    transformation = BronzeToSilver(
        config=config,
        source_table="sales",
    )
    return transformation.execute("bronze", "silver")

# 3. Build pipeline
pipeline = Pipeline("sales_pipeline", config)
pipeline.add_task(Task("ingest", ingest))
pipeline.add_task(Task("transform", transform, dependencies=["ingest"]))

# 4. Execute
results = pipeline.run()
pipeline.print_summary()
```

## Core Components

### 1. Configuration Management

Handles Unity Catalog three-level namespace (`catalog.schema.table`) patterns.

```python
from end2end import PipelineConfig

# Basic configuration
config = PipelineConfig(
    catalog="databricks_course",
    source_schema="my_schema",
    environment="dev"
)

# Build table paths
bronze_path = config.get_table_path("bronze", "sales")
# Result: "databricks_course.my_schema.bronze_sales"

silver_path = config.get_table_path("silver", "sales")
# Result: "databricks_course.my_schema.silver_sales"
```

**User-Specific Configuration:**

```python
from end2end import create_user_config

# Automatically derive schema from user email
config = create_user_config("john.doe@company.com")
# source_schema: "john_doe"
```

### 2. Data Ingestion

Multi-source ingestion with validation and retry logic.

#### File Ingestion

```python
from end2end import FileIngestion
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema (best practice)
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DoubleType(), False),
])

# Create ingestion
ingestion = FileIngestion(
    config=config,
    source_path="/data/sales.csv",
    file_format="csv",
    schema=schema,
    options={"header": "true", "delimiter": ","}
)

# Execute with retry logic
result = ingestion.execute("sales_transactions", write_mode="append")
print(f"Ingested {result['rows_read']} rows")
```

#### API Ingestion

```python
from end2end import APIIngestion

# Configure API ingestion
api_ingestion = APIIngestion(
    config=config,
    api_url="https://api.example.com/sales",
    auth_token="your-api-key",
    params={"date": "2024-01-01"},
    rate_limit_delay=0.1  # 100ms between requests
)

# Execute
result = api_ingestion.execute("sales_api_data")
```

#### Database Ingestion

```python
from end2end import DatabaseIngestion

# Configure JDBC ingestion
db_ingestion = DatabaseIngestion(
    config=config,
    jdbc_url="jdbc:postgresql://localhost:5432/mydb",
    table_or_query="sales_transactions",
    connection_properties={
        "user": "db_user",
        "password": "db_password",
        "driver": "org.postgresql.Driver"
    },
    partition_column="id",
    num_partitions=8
)

# Execute
result = db_ingestion.execute("sales_from_db")
```

### 3. Transformations

Medallion architecture transformations (Bronze → Silver → Gold).

#### Bronze to Silver

```python
from end2end import BronzeToSilver

# Configure cleaning transformation
transformation = BronzeToSilver(
    config=config,
    source_table="sales_transactions",
    remove_duplicates=True,
    required_columns=["transaction_id", "amount"],
    transformations={
        "email": "lower(trim(email))",
        "status": "upper(status)"
    }
)

# Execute
result = transformation.execute("bronze", "silver")
print(f"Transformed {result['rows_read']} -> {result['rows_written']} rows")
```

#### Silver to Gold

```python
from end2end import SilverToGold

# Configure aggregation
aggregation = SilverToGold(
    config=config,
    source_table="sales_transactions",
    target_table="daily_sales_summary",
    group_by_columns=["date", "region"],
    aggregations={
        "amount": ["sum", "avg", "count"],
        "quantity": ["sum"]
    }
)

# Execute
result = aggregation.execute("silver", "gold")
```

### 4. Data Quality

Schema validation and data quality checks.

#### Schema Validation

```python
from end2end import SchemaValidator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define expected schema
expected_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("amount", DoubleType(), False),
])

# Validate DataFrame
validator = SchemaValidator(expected_schema)
is_valid = validator.validate(df)

if not is_valid:
    diff = validator.get_schema_diff(df)
    print(f"Schema differences: {diff}")
```

#### Data Quality Checks

```python
from end2end import DataQualityCheck

# Run comprehensive quality checks
checker = DataQualityCheck(df)
report = checker.run_all_checks()

print(f"Quality Score: {report['quality_score']:.2%}")
print(f"Completeness: {report['completeness_score']:.2%}")

# Print detailed report
checker.print_report()
```

#### Expectations

```python
from end2end import Expectation

# Define expectations
amount_expectation = Expectation("amount")
amount_expectation.expect_column_to_exist() \
                  .expect_column_values_to_not_be_null() \
                  .expect_column_values_to_be_between(0, 1000000)

# Validate
result = amount_expectation.validate(df)
print(f"Passed: {result['passed']}/{result['total_expectations']}")
```

### 5. Pipeline Orchestration

DAG-based workflow execution with automatic dependency resolution.

```python
from end2end import Pipeline, Task

# Create pipeline
pipeline = Pipeline("my_pipeline", config)

# Add tasks with dependencies
pipeline.add_task(Task("ingest", ingest_function, retry_attempts=2))
pipeline.add_task(Task("transform", transform_function, dependencies=["ingest"]))
pipeline.add_task(Task("aggregate", aggregate_function, dependencies=["transform"]))

# Validate dependencies
pipeline.validate_dependencies()

# View execution plan
execution_order = pipeline.get_execution_order()
print(f"Execution plan: {execution_order}")

# Execute pipeline
results = pipeline.run()
pipeline.print_summary()
```

## Best Practices

### Configuration

- Always use `PipelineConfig` for Unity Catalog namespace management
- Separate dev/staging/prod configurations
- Store sensitive credentials in Databricks secrets

### Data Ingestion

- Define explicit schemas (avoid schema inference)
- Implement retry logic for external sources
- Validate data immediately after ingestion
- Use appropriate file formats (Parquet for analytics)

### Transformations

- Follow medallion architecture strictly (Bronze → Silver → Gold)
- Keep transformations idempotent
- Add metadata columns (processed_at, version, etc.)
- Document business logic in code comments

### Data Quality

- Validate schemas before transformations
- Implement expectation-based testing
- Monitor quality metrics over time
- Set up alerts for quality failures

### Pipeline Orchestration

- Define clear task dependencies
- Implement retry logic for transient failures
- Log execution metrics comprehensively
- Use dry_run for pipeline validation before production

### Performance

- Optimize Delta tables regularly with `OPTIMIZE`
- Use Z-ordering on frequently filtered columns
- Partition large tables appropriately
- Cache frequently accessed data

## Examples

### Complete Production Pipeline

See `course/notebooks/advanced/04_end2end_production_pipeline.py` for a complete real-world example including:

- Multi-source ingestion
- Data cleaning and validation
- Multiple aggregation layers
- Quality checks
- Performance optimization
- Monitoring and metrics

### Building a Wheel Package

```bash
# Navigate to project root
cd /path/to/databricks-infra-org

# Build wheel
poetry build

# Output: dist/databricks_infra-0.1.0-py3-none-any.whl
```

### Deploying to Databricks

```python
# Upload to Unity Catalog Volumes
dbutils.fs.cp(
    "file:/Workspace/Users/your.email@example.com/databricks_infra-0.1.0-py3-none-any.whl",
    "dbfs:/Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl"
)

# Install in notebook
%pip install /Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl

# Or use in Databricks Job
# Add as library: /Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl
```

## API Reference

See individual module documentation:

- `config.py`: Configuration classes and functions
- `ingestion/`: Ingestion classes (File, API, Database)
- `transformations/`: Transformation classes (BronzeToSilver, SilverToGold)
- `quality/`: Validation and quality check classes
- `pipeline/`: Pipeline and Task classes
- `utils/`: Logging and Spark utilities

## Troubleshooting

### Import Errors

**Problem:** `ModuleNotFoundError: No module named 'end2end'`

**Solution:**

```python
# For development, add src to path
import sys
import os
sys.path.insert(0, "/path/to/src")

# For production, install wheel package
%pip install /Volumes/databricks_course/shared/packages/databricks_infra-0.1.0-py3-none-any.whl
```

### Configuration Errors

**Problem:** `ValueError: Invalid catalog name`

**Solution:** Ensure catalog/schema names follow Unity Catalog naming rules (alphanumeric + underscore only).

### Performance Issues

**Problem:** Slow transformations or queries

**Solutions:**
- Run `OPTIMIZE` on Delta tables
- Use Z-ordering on frequently filtered columns
- Check partition strategy
- Review Spark configuration

## Contributing

The framework is designed to be extended. To add custom components:

1. **Custom Ingestion:**
   ```python
   from end2end.ingestion.base import BaseIngestion

   class MyIngestion(BaseIngestion):
       def read_source(self):
           # Your custom logic
           return df
   ```

2. **Custom Transformation:**
   ```python
   from end2end.transformations.base import BaseTransformation

   class MyTransformation(BaseTransformation):
       def transform(self, df):
           # Your custom logic
           return transformed_df
   ```

## Resources

- **Documentation:** `docs/END2END_FRAMEWORK_GUIDE.md` (this file)
- **Tutorial Notebook:** `course/notebooks/advanced/03_end2end_framework_guide.py`
- **Production Example:** `course/notebooks/advanced/04_end2end_production_pipeline.py`
- **Source Code:** `src/end2end/`
- **Course Materials:** Weeks 1-5 notebooks for pattern details

## License

Part of the Databricks Infrastructure course project.

## Support

For questions or issues, refer to the course materials or contact the course instructors.