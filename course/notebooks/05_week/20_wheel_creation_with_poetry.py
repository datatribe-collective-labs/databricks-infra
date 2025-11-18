# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 - Notebook 20: Creating Python Wheels with Poetry
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Understand when and why to use Python wheels in Databricks
# MAGIC - Set up a Poetry-based Python project for data engineering
# MAGIC - Create reusable modules for ingestion and transformation logic
# MAGIC - Build wheel files locally using Poetry
# MAGIC - Upload and install wheels in Databricks environments
# MAGIC - Use wheels in Databricks Jobs for production deployment
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completion of Week 2-4 notebooks (ingestion and transformations)
# MAGIC - Understanding of Python modules and packages
# MAGIC - Familiarity with command-line tools (for local development)
# MAGIC
# MAGIC ## Notebook Structure
# MAGIC - **Part 1**: Wheel Concepts and When to Use Them
# MAGIC - **Part 2**: Project Structure for Databricks Packages
# MAGIC - **Part 3**: Poetry Setup and Configuration
# MAGIC - **Part 4**: Writing Reusable Data Engineering Code
# MAGIC - **Part 5**: Building Wheels Locally
# MAGIC - **Part 6**: Deploying Wheels to Databricks
# MAGIC - **Part 7**: Using Wheels in Jobs and Notebooks
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Understanding Python Wheels for Databricks
# MAGIC
# MAGIC ## What is a Python Wheel?
# MAGIC
# MAGIC A **wheel** is a built-package format for Python that allows you to:
# MAGIC - Package your code into a distributable format (`.whl` file)
# MAGIC - Include dependencies and metadata
# MAGIC - Install quickly without compilation
# MAGIC - Share code across notebooks, jobs, and clusters
# MAGIC
# MAGIC ## Notebooks vs. Wheels: When to Use Each
# MAGIC
# MAGIC | Approach | Use Cases | Pros | Cons |
# MAGIC |----------|-----------|------|------|
# MAGIC | **Notebooks** | - Exploratory analysis<br>- Quick prototypes<br>- Educational content<br>- Interactive development | - Easy to write and test<br>- Visual feedback<br>- Great for iteration | - Hard to version control<br>- Difficult to test<br>- Not reusable across jobs |
# MAGIC | **Wheels** | - Production jobs<br>- Shared libraries<br>- Reusable utilities<br>- Complex logic | - Version controlled<br>- Unit testable<br>- Reusable everywhere<br>- Professional deployment | - Requires build process<br>- More setup overhead |
# MAGIC
# MAGIC ## When to Create a Wheel for Databricks
# MAGIC
# MAGIC ✅ **Create a wheel when**:
# MAGIC - You have logic used across multiple notebooks or jobs
# MAGIC - You need version control for data engineering utilities
# MAGIC - You want to write unit tests for your transformations
# MAGIC - You're building production pipelines that need reliability
# MAGIC - You want to separate business logic from orchestration
# MAGIC
# MAGIC ❌ **Stick with notebooks when**:
# MAGIC - You're doing one-off exploratory analysis
# MAGIC - The logic is specific to a single use case
# MAGIC - You're still prototyping and iterating rapidly
# MAGIC - The audience needs to see visual outputs and explanations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-World Example: From Notebooks to Wheels
# MAGIC
# MAGIC In our course, we've built ingestion pipelines in Notebooks 06-09:
# MAGIC - `06_file_ingestion.py` - CSV, JSON, Parquet ingestion
# MAGIC - `07_api_ingest.py` - REST API data fetching
# MAGIC - `08_database_ingest.py` - JDBC database connections
# MAGIC - `09_s3_ingest.py` - Cloud storage ingestion
# MAGIC
# MAGIC Each notebook contains logic that could be reused. Let's package common patterns:
# MAGIC
# MAGIC ```python
# MAGIC # Instead of copying this code to every notebook:
# MAGIC def ingest_csv_with_schema(file_path, schema, table_name):
# MAGIC     df = spark.read.format("csv").schema(schema).option("header", True).load(file_path)
# MAGIC     df.write.mode("overwrite").saveAsTable(table_name)
# MAGIC     return df
# MAGIC
# MAGIC # Package it in a wheel and use it everywhere:
# MAGIC from databricks_course_utils.ingestion import ingest_csv_with_schema
# MAGIC
# MAGIC df = ingest_csv_with_schema(
# MAGIC     file_path="dbfs:/mnt/data/sales.csv",
# MAGIC     schema=sales_schema,
# MAGIC     table_name="bronze_sales"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This approach:
# MAGIC - Reduces code duplication
# MAGIC - Makes testing easier (unit test the function once)
# MAGIC - Allows versioning (wheel v1.0.0, v1.1.0, etc.)
# MAGIC - Enables collaboration (team members use same utilities)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Project Structure for Databricks Packages
# MAGIC
# MAGIC ## Recommended Directory Layout
# MAGIC
# MAGIC For a production-ready Databricks Python package, use this structure:
# MAGIC
# MAGIC ```
# MAGIC databricks-course-utils/
# MAGIC ├── src/
# MAGIC │   └── databricks_course_utils/
# MAGIC │       ├── __init__.py              # Package initialization
# MAGIC │       ├── ingestion/
# MAGIC │       │   ├── __init__.py
# MAGIC │       │   ├── file_ingest.py       # CSV, JSON, Parquet utilities
# MAGIC │       │   ├── api_ingest.py        # REST API fetching
# MAGIC │       │   ├── database_ingest.py   # JDBC utilities
# MAGIC │       │   └── cloud_ingest.py      # S3, ADLS utilities
# MAGIC │       ├── transformations/
# MAGIC │       │   ├── __init__.py
# MAGIC │       │   ├── cleansing.py         # Data cleaning functions
# MAGIC │       │   ├── enrichment.py        # Data enrichment
# MAGIC │       │   └── aggregations.py      # Business metrics
# MAGIC │       └── utils/
# MAGIC │           ├── __init__.py
# MAGIC │           ├── schema_helpers.py    # Schema utilities
# MAGIC │           └── validators.py        # Data quality checks
# MAGIC ├── tests/
# MAGIC │   ├── test_ingestion.py
# MAGIC │   ├── test_transformations.py
# MAGIC │   └── test_utils.py
# MAGIC ├── pyproject.toml                   # Poetry configuration
# MAGIC ├── README.md                        # Documentation
# MAGIC └── .gitignore
# MAGIC ```
# MAGIC
# MAGIC ## Why This Structure?
# MAGIC
# MAGIC - **src/ layout**: Modern Python packaging best practice, prevents accidental imports
# MAGIC - **Modular organization**: Ingestion, transformations, and utilities are separated
# MAGIC - **Clear hierarchy**: Easy to find and maintain code
# MAGIC - **Testable**: Tests mirror the source structure
# MAGIC - **Scalable**: Easy to add new modules as project grows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: Creating the Project Structure
# MAGIC
# MAGIC Here's how to create this structure locally (run these commands in your terminal):
# MAGIC
# MAGIC ```bash
# MAGIC # Create project directory
# MAGIC mkdir -p databricks-course-utils
# MAGIC cd databricks-course-utils
# MAGIC
# MAGIC # Create source structure
# MAGIC mkdir -p src/databricks_course_utils/{ingestion,transformations,utils}
# MAGIC touch src/databricks_course_utils/__init__.py
# MAGIC touch src/databricks_course_utils/ingestion/__init__.py
# MAGIC touch src/databricks_course_utils/transformations/__init__.py
# MAGIC touch src/databricks_course_utils/utils/__init__.py
# MAGIC
# MAGIC # Create test structure
# MAGIC mkdir -p tests
# MAGIC touch tests/__init__.py
# MAGIC
# MAGIC # Create configuration files
# MAGIC touch pyproject.toml README.md .gitignore
# MAGIC ```
# MAGIC
# MAGIC **Result**: You now have a professional Python package structure ready for development.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Poetry Setup and Configuration
# MAGIC
# MAGIC ## What is Poetry?
# MAGIC
# MAGIC **Poetry** is a modern Python dependency and packaging tool that simplifies:
# MAGIC - Dependency management (replaces `requirements.txt` and `setup.py`)
# MAGIC - Virtual environment management
# MAGIC - Package building (creating wheels)
# MAGIC - Publishing to PyPI or private repositories
# MAGIC
# MAGIC ## Installing Poetry
# MAGIC
# MAGIC On your local development machine, install Poetry:
# MAGIC
# MAGIC ```bash
# MAGIC # macOS / Linux / WSL
# MAGIC curl -sSL https://install.python-poetry.org | python3 -
# MAGIC
# MAGIC # Windows PowerShell
# MAGIC (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
# MAGIC
# MAGIC # Verify installation
# MAGIC poetry --version
# MAGIC # Output: Poetry (version 1.7.0)
# MAGIC ```
# MAGIC
# MAGIC ## Initializing a Poetry Project
# MAGIC
# MAGIC ```bash
# MAGIC # Navigate to your project directory
# MAGIC cd databricks-course-utils
# MAGIC
# MAGIC # Initialize Poetry (interactive)
# MAGIC poetry init
# MAGIC
# MAGIC # Follow prompts:
# MAGIC # Package name: databricks-course-utils
# MAGIC # Version: 0.1.0
# MAGIC # Description: Reusable utilities for Databricks data engineering
# MAGIC # Author: Your Name <your.email@example.com>
# MAGIC # License: MIT
# MAGIC # Python version: ^3.11
# MAGIC # Dependencies: (we'll add these next)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating pyproject.toml
# MAGIC
# MAGIC The `pyproject.toml` file is the heart of your Poetry project. Here's a complete example:
# MAGIC
# MAGIC ```toml
# MAGIC [tool.poetry]
# MAGIC name = "databricks-course-utils"
# MAGIC version = "0.1.0"
# MAGIC description = "Reusable data engineering utilities for Databricks"
# MAGIC authors = ["Your Name <your.email@example.com>"]
# MAGIC readme = "README.md"
# MAGIC packages = [{include = "databricks_course_utils", from = "src"}]
# MAGIC
# MAGIC [tool.poetry.dependencies]
# MAGIC python = "^3.11"
# MAGIC pyspark = "^3.5.0"  # Match your Databricks runtime
# MAGIC delta-spark = "^3.0.0"
# MAGIC requests = "^2.31.0"  # For API ingestion
# MAGIC
# MAGIC [tool.poetry.group.dev.dependencies]
# MAGIC pytest = "^7.4.0"
# MAGIC black = "^23.12.0"
# MAGIC ruff = "^0.1.9"
# MAGIC mypy = "^1.8.0"
# MAGIC
# MAGIC [build-system]
# MAGIC requires = ["poetry-core"]
# MAGIC build-backend = "poetry.core.masonry.api"
# MAGIC ```
# MAGIC
# MAGIC ## Key Sections Explained
# MAGIC
# MAGIC - **[tool.poetry]**: Package metadata and configuration
# MAGIC   - `packages = [{include = "databricks_course_utils", from = "src"}]`: Tells Poetry where to find your code
# MAGIC - **[tool.poetry.dependencies]**: Runtime dependencies (included in the wheel)
# MAGIC   - `python = "^3.11"`: Python version constraint
# MAGIC   - `pyspark = "^3.5.0"`: Must match Databricks runtime version
# MAGIC - **[tool.poetry.group.dev.dependencies]**: Development-only dependencies (not included in wheel)
# MAGIC   - Testing, linting, formatting tools
# MAGIC - **[build-system]**: Build configuration for creating wheels

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installing Dependencies
# MAGIC
# MAGIC After creating `pyproject.toml`, install dependencies:
# MAGIC
# MAGIC ```bash
# MAGIC # Install all dependencies (creates virtual environment)
# MAGIC poetry install
# MAGIC
# MAGIC # Activate virtual environment
# MAGIC poetry shell
# MAGIC
# MAGIC # Verify installation
# MAGIC poetry show
# MAGIC # Lists all installed packages
# MAGIC
# MAGIC # Add a new dependency
# MAGIC poetry add pandas
# MAGIC
# MAGIC # Add a development dependency
# MAGIC poetry add --group dev ipython
# MAGIC
# MAGIC # Update dependencies
# MAGIC poetry update
# MAGIC ```
# MAGIC
# MAGIC ## Version Management Best Practices
# MAGIC
# MAGIC ```toml
# MAGIC # Pin exact version (use for critical dependencies)
# MAGIC pyspark = "3.5.0"
# MAGIC
# MAGIC # Allow patch updates (recommended)
# MAGIC requests = "^2.31.0"  # Allows 2.31.x, not 2.32.0
# MAGIC
# MAGIC # Allow minor updates (more flexible)
# MAGIC pandas = "~2.1.0"  # Allows 2.1.x, not 2.2.0
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4: Writing Reusable Data Engineering Code
# MAGIC
# MAGIC Now let's write actual modules based on patterns from our course notebooks.
# MAGIC
# MAGIC ## Module 1: File Ingestion Utilities
# MAGIC
# MAGIC **File**: `src/databricks_course_utils/ingestion/file_ingest.py`
# MAGIC
# MAGIC ```python
# MAGIC """File-based ingestion utilities for CSV, JSON, and Parquet formats."""
# MAGIC
# MAGIC from pyspark.sql import SparkSession, DataFrame
# MAGIC from pyspark.sql.types import StructType
# MAGIC from typing import Optional, Dict, Any
# MAGIC
# MAGIC
# MAGIC def ingest_csv(
# MAGIC     spark: SparkSession,
# MAGIC     file_path: str,
# MAGIC     schema: Optional[StructType] = None,
# MAGIC     options: Optional[Dict[str, Any]] = None
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Ingest CSV file with optional schema and custom options.
# MAGIC
# MAGIC     Args:
# MAGIC         spark: SparkSession instance
# MAGIC         file_path: Path to CSV file (dbfs:/ or /Volumes/)
# MAGIC         schema: Optional StructType schema for explicit typing
# MAGIC         options: Optional dict of CSV reader options
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame containing CSV data
# MAGIC
# MAGIC     Example:
# MAGIC         >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC         >>> schema = StructType([
# MAGIC         ...     StructField("id", IntegerType(), False),
# MAGIC         ...     StructField("name", StringType(), True)
# MAGIC         ... ])
# MAGIC         >>> df = ingest_csv(spark, "dbfs:/data/customers.csv", schema=schema)
# MAGIC     """
# MAGIC     default_options = {"header": "true", "inferSchema": "false"}
# MAGIC     if options:
# MAGIC         default_options.update(options)
# MAGIC
# MAGIC     reader = spark.read.format("csv").options(**default_options)
# MAGIC
# MAGIC     if schema:
# MAGIC         reader = reader.schema(schema)
# MAGIC
# MAGIC     return reader.load(file_path)
# MAGIC
# MAGIC
# MAGIC def ingest_json(
# MAGIC     spark: SparkSession,
# MAGIC     file_path: str,
# MAGIC     schema: Optional[StructType] = None,
# MAGIC     multiline: bool = False
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Ingest JSON file with optional schema.
# MAGIC
# MAGIC     Args:
# MAGIC         spark: SparkSession instance
# MAGIC         file_path: Path to JSON file
# MAGIC         schema: Optional StructType schema
# MAGIC         multiline: Whether JSON spans multiple lines
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame containing JSON data
# MAGIC     """
# MAGIC     reader = spark.read.format("json").option("multiLine", str(multiline).lower())
# MAGIC
# MAGIC     if schema:
# MAGIC         reader = reader.schema(schema)
# MAGIC
# MAGIC     return reader.load(file_path)
# MAGIC
# MAGIC
# MAGIC def ingest_parquet(
# MAGIC     spark: SparkSession,
# MAGIC     file_path: str,
# MAGIC     columns: Optional[list] = None
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Ingest Parquet file with optional column selection.
# MAGIC
# MAGIC     Args:
# MAGIC         spark: SparkSession instance
# MAGIC         file_path: Path to Parquet file
# MAGIC         columns: Optional list of columns to select
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame containing Parquet data
# MAGIC     """
# MAGIC     df = spark.read.format("parquet").load(file_path)
# MAGIC
# MAGIC     if columns:
# MAGIC         df = df.select(*columns)
# MAGIC
# MAGIC     return df
# MAGIC
# MAGIC
# MAGIC def save_to_delta(
# MAGIC     df: DataFrame,
# MAGIC     table_name: str,
# MAGIC     mode: str = "overwrite",
# MAGIC     partition_by: Optional[list] = None
# MAGIC ) -> None:
# MAGIC     """
# MAGIC     Save DataFrame to Delta table with optional partitioning.
# MAGIC
# MAGIC     Args:
# MAGIC         df: DataFrame to save
# MAGIC         table_name: Three-level namespace (catalog.schema.table)
# MAGIC         mode: Write mode (overwrite, append, etc.)
# MAGIC         partition_by: Optional list of columns to partition by
# MAGIC
# MAGIC     Example:
# MAGIC         >>> save_to_delta(
# MAGIC         ...     df=sales_df,
# MAGIC         ...     table_name="databricks_course.user_schema.bronze_sales",
# MAGIC         ...     mode="append",
# MAGIC         ...     partition_by=["transaction_date"]
# MAGIC         ... )
# MAGIC     """
# MAGIC     writer = df.write.format("delta").mode(mode)
# MAGIC
# MAGIC     if partition_by:
# MAGIC         writer = writer.partitionBy(*partition_by)
# MAGIC
# MAGIC     writer.saveAsTable(table_name)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 2: Transformation Utilities
# MAGIC
# MAGIC **File**: `src/databricks_course_utils/transformations/cleansing.py`
# MAGIC
# MAGIC ```python
# MAGIC """Data cleansing and quality utilities."""
# MAGIC
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import col, trim, upper, lower, regexp_replace, when, coalesce, lit
# MAGIC from typing import List, Optional
# MAGIC
# MAGIC
# MAGIC def remove_duplicates(
# MAGIC     df: DataFrame,
# MAGIC     subset: Optional[List[str]] = None,
# MAGIC     keep: str = "first"
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Remove duplicate rows from DataFrame.
# MAGIC
# MAGIC     Args:
# MAGIC         df: Input DataFrame
# MAGIC         subset: Optional list of columns to consider for duplicates
# MAGIC         keep: Which duplicate to keep ('first' or 'last')
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame with duplicates removed
# MAGIC     """
# MAGIC     if subset:
# MAGIC         return df.dropDuplicates(subset)
# MAGIC     return df.dropDuplicates()
# MAGIC
# MAGIC
# MAGIC def standardize_string_columns(
# MAGIC     df: DataFrame,
# MAGIC     columns: List[str],
# MAGIC     case: str = "upper",
# MAGIC     trim_spaces: bool = True
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Standardize string columns (trim, case normalization).
# MAGIC
# MAGIC     Args:
# MAGIC         df: Input DataFrame
# MAGIC         columns: List of column names to standardize
# MAGIC         case: 'upper', 'lower', or 'none'
# MAGIC         trim_spaces: Whether to trim leading/trailing spaces
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame with standardized string columns
# MAGIC     """
# MAGIC     for column in columns:
# MAGIC         if trim_spaces:
# MAGIC             df = df.withColumn(column, trim(col(column)))
# MAGIC
# MAGIC         if case == "upper":
# MAGIC             df = df.withColumn(column, upper(col(column)))
# MAGIC         elif case == "lower":
# MAGIC             df = df.withColumn(column, lower(col(column)))
# MAGIC
# MAGIC     return df
# MAGIC
# MAGIC
# MAGIC def fill_missing_values(
# MAGIC     df: DataFrame,
# MAGIC     column: str,
# MAGIC     fill_value: any = None,
# MAGIC     fill_with_column: Optional[str] = None
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Fill missing values in a column.
# MAGIC
# MAGIC     Args:
# MAGIC         df: Input DataFrame
# MAGIC         column: Column name to fill
# MAGIC         fill_value: Static value to use for filling
# MAGIC         fill_with_column: Alternative column to use for filling
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame with filled values
# MAGIC     """
# MAGIC     if fill_with_column:
# MAGIC         df = df.withColumn(column, coalesce(col(column), col(fill_with_column)))
# MAGIC     elif fill_value is not None:
# MAGIC         df = df.withColumn(column, coalesce(col(column), lit(fill_value)))
# MAGIC
# MAGIC     return df
# MAGIC
# MAGIC
# MAGIC def validate_data_quality(
# MAGIC     df: DataFrame,
# MAGIC     required_columns: List[str],
# MAGIC     non_null_columns: Optional[List[str]] = None
# MAGIC ) -> tuple[DataFrame, DataFrame]:
# MAGIC     """
# MAGIC     Validate data quality and separate good/bad records.
# MAGIC
# MAGIC     Args:
# MAGIC         df: Input DataFrame
# MAGIC         required_columns: Columns that must exist
# MAGIC         non_null_columns: Columns that cannot have null values
# MAGIC
# MAGIC     Returns:
# MAGIC         Tuple of (good_records_df, bad_records_df)
# MAGIC     """
# MAGIC     # Check required columns exist
# MAGIC     missing_cols = set(required_columns) - set(df.columns)
# MAGIC     if missing_cols:
# MAGIC         raise ValueError(f"Missing required columns: {missing_cols}")
# MAGIC
# MAGIC     # Build quality filter
# MAGIC     if non_null_columns:
# MAGIC         quality_condition = col(non_null_columns[0]).isNotNull()
# MAGIC         for column in non_null_columns[1:]:
# MAGIC             quality_condition = quality_condition & col(column).isNotNull()
# MAGIC
# MAGIC         good_records = df.filter(quality_condition)
# MAGIC         bad_records = df.filter(~quality_condition)
# MAGIC     else:
# MAGIC         good_records = df
# MAGIC         bad_records = df.filter(lit(False))  # Empty DataFrame
# MAGIC
# MAGIC     return good_records, bad_records
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 3: Package Initialization
# MAGIC
# MAGIC **File**: `src/databricks_course_utils/__init__.py`
# MAGIC
# MAGIC ```python
# MAGIC """Databricks Course Utilities - Reusable data engineering components."""
# MAGIC
# MAGIC __version__ = "0.1.0"
# MAGIC
# MAGIC # Export key functions for easy importing
# MAGIC from databricks_course_utils.ingestion.file_ingest import (
# MAGIC     ingest_csv,
# MAGIC     ingest_json,
# MAGIC     ingest_parquet,
# MAGIC     save_to_delta
# MAGIC )
# MAGIC
# MAGIC from databricks_course_utils.transformations.cleansing import (
# MAGIC     remove_duplicates,
# MAGIC     standardize_string_columns,
# MAGIC     fill_missing_values,
# MAGIC     validate_data_quality
# MAGIC )
# MAGIC
# MAGIC __all__ = [
# MAGIC     # Ingestion
# MAGIC     "ingest_csv",
# MAGIC     "ingest_json",
# MAGIC     "ingest_parquet",
# MAGIC     "save_to_delta",
# MAGIC     # Transformations
# MAGIC     "remove_duplicates",
# MAGIC     "standardize_string_columns",
# MAGIC     "fill_missing_values",
# MAGIC     "validate_data_quality",
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC **Why this pattern?**
# MAGIC - Users can import directly: `from databricks_course_utils import ingest_csv`
# MAGIC - Explicit exports via `__all__` for clarity
# MAGIC - Version tracking with `__version__`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 4: Unit Tests
# MAGIC
# MAGIC **File**: `tests/test_ingestion.py`
# MAGIC
# MAGIC ```python
# MAGIC """Unit tests for ingestion utilities."""
# MAGIC
# MAGIC import pytest
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC from databricks_course_utils.ingestion.file_ingest import ingest_csv, save_to_delta
# MAGIC
# MAGIC
# MAGIC @pytest.fixture(scope="session")
# MAGIC def spark():
# MAGIC     """Create SparkSession for testing."""
# MAGIC     return SparkSession.builder \
# MAGIC         .appName("test") \
# MAGIC         .master("local[*]") \
# MAGIC         .getOrCreate()
# MAGIC
# MAGIC
# MAGIC def test_ingest_csv_with_schema(spark, tmp_path):
# MAGIC     """Test CSV ingestion with explicit schema."""
# MAGIC     # Create test CSV file
# MAGIC     csv_path = tmp_path / "test.csv"
# MAGIC     csv_path.write_text("id,name\n1,Alice\n2,Bob")
# MAGIC
# MAGIC     # Define schema
# MAGIC     schema = StructType([
# MAGIC         StructField("id", IntegerType(), False),
# MAGIC         StructField("name", StringType(), True)
# MAGIC     ])
# MAGIC
# MAGIC     # Test ingestion
# MAGIC     df = ingest_csv(spark, str(csv_path), schema=schema)
# MAGIC
# MAGIC     # Assertions
# MAGIC     assert df.count() == 2
# MAGIC     assert df.schema == schema
# MAGIC     assert df.collect()[0]["name"] == "Alice"
# MAGIC
# MAGIC
# MAGIC def test_ingest_csv_with_options(spark, tmp_path):
# MAGIC     """Test CSV ingestion with custom delimiter."""
# MAGIC     # Create test CSV with pipe delimiter
# MAGIC     csv_path = tmp_path / "test_pipe.csv"
# MAGIC     csv_path.write_text("id|name\n1|Alice\n2|Bob")
# MAGIC
# MAGIC     # Test with custom delimiter
# MAGIC     df = ingest_csv(
# MAGIC         spark,
# MAGIC         str(csv_path),
# MAGIC         options={"delimiter": "|"}
# MAGIC     )
# MAGIC
# MAGIC     assert df.count() == 2
# MAGIC     assert "name" in df.columns
# MAGIC ```
# MAGIC
# MAGIC **Run tests locally**:
# MAGIC ```bash
# MAGIC poetry run pytest tests/ -v
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 5: Building Wheels Locally
# MAGIC
# MAGIC Now that we have our code written, let's build the wheel file.
# MAGIC
# MAGIC ## Building with Poetry
# MAGIC
# MAGIC ```bash
# MAGIC # Ensure you're in the project directory
# MAGIC cd databricks-course-utils
# MAGIC
# MAGIC # Build the wheel (creates .whl file in dist/)
# MAGIC poetry build
# MAGIC
# MAGIC # Output:
# MAGIC # Building databricks-course-utils (0.1.0)
# MAGIC #   - Building sdist
# MAGIC #   - Built databricks_course_utils-0.1.0.tar.gz
# MAGIC #   - Building wheel
# MAGIC #   - Built databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC ## Understanding the Output
# MAGIC
# MAGIC Poetry creates two files in the `dist/` directory:
# MAGIC
# MAGIC 1. **Source distribution** (`.tar.gz`):
# MAGIC    - Contains raw source code
# MAGIC    - Requires installation step
# MAGIC    - Used for PyPI publishing
# MAGIC
# MAGIC 2. **Wheel** (`.whl`):
# MAGIC    - Pre-built package
# MAGIC    - Fast installation
# MAGIC    - **This is what we'll use in Databricks**
# MAGIC
# MAGIC ## Wheel Filename Breakdown
# MAGIC
# MAGIC ```
# MAGIC databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC │                       │      │   │    │
# MAGIC │                       │      │   │    └─ Platform: any (pure Python)
# MAGIC │                       │      │   └────── ABI: none (no C extensions)
# MAGIC │                       │      └────────── Python version: py3 (Python 3.x)
# MAGIC │                       └───────────────── Package version: 0.1.0
# MAGIC └───────────────────────────────────────── Package name
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Versioning Your Wheel
# MAGIC
# MAGIC Use **semantic versioning** (MAJOR.MINOR.PATCH):
# MAGIC
# MAGIC - **MAJOR**: Breaking changes (1.0.0 → 2.0.0)
# MAGIC - **MINOR**: New features, backwards compatible (1.0.0 → 1.1.0)
# MAGIC - **PATCH**: Bug fixes (1.0.0 → 1.0.1)
# MAGIC
# MAGIC ```bash
# MAGIC # Update version in pyproject.toml
# MAGIC poetry version patch  # 0.1.0 → 0.1.1
# MAGIC poetry version minor  # 0.1.1 → 0.2.0
# MAGIC poetry version major  # 0.2.0 → 1.0.0
# MAGIC
# MAGIC # Rebuild with new version
# MAGIC poetry build
# MAGIC ```
# MAGIC
# MAGIC ## Quality Checks Before Building
# MAGIC
# MAGIC Always run these checks before building:
# MAGIC
# MAGIC ```bash
# MAGIC # Run tests
# MAGIC poetry run pytest tests/ -v
# MAGIC
# MAGIC # Format code
# MAGIC poetry run black src/ tests/
# MAGIC
# MAGIC # Lint code
# MAGIC poetry run ruff check src/ tests/
# MAGIC
# MAGIC # Type checking
# MAGIC poetry run mypy src/
# MAGIC
# MAGIC # If all pass, build
# MAGIC poetry build
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing the Wheel Locally (Before Databricks)
# MAGIC
# MAGIC Before uploading to Databricks, test the wheel locally:
# MAGIC
# MAGIC ```bash
# MAGIC # Create a fresh virtual environment for testing
# MAGIC python -m venv test_env
# MAGIC source test_env/bin/activate  # On Windows: test_env\Scripts\activate
# MAGIC
# MAGIC # Install the wheel
# MAGIC pip install dist/databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC
# MAGIC # Test imports
# MAGIC python -c "from databricks_course_utils import ingest_csv; print('✅ Import successful')"
# MAGIC
# MAGIC # Test with PySpark (if installed)
# MAGIC python << EOF
# MAGIC from pyspark.sql import SparkSession
# MAGIC from databricks_course_utils import ingest_csv
# MAGIC
# MAGIC spark = SparkSession.builder.appName("test").getOrCreate()
# MAGIC print("✅ Package works with PySpark")
# MAGIC EOF
# MAGIC
# MAGIC # Deactivate test environment
# MAGIC deactivate
# MAGIC ```
# MAGIC
# MAGIC If local testing passes, you're ready to deploy to Databricks! ✅

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 6: Deploying Wheels to Databricks
# MAGIC
# MAGIC There are three main ways to deploy wheels to Databricks:
# MAGIC 1. **Databricks UI** (easiest for beginners)
# MAGIC 2. **Databricks CLI** (good for automation)
# MAGIC 3. **Unity Catalog Volumes** (modern, recommended for production)
# MAGIC
# MAGIC ## Option 1: Upload via Databricks UI
# MAGIC
# MAGIC **Step-by-step process**:
# MAGIC
# MAGIC 1. **Build your wheel locally**:
# MAGIC    ```bash
# MAGIC    poetry build
# MAGIC    # Wheel created: dist/databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC    ```
# MAGIC
# MAGIC 2. **Navigate to Databricks workspace**:
# MAGIC    - Go to: https://your-workspace.cloud.databricks.com
# MAGIC    - Click **Workspace** in left sidebar
# MAGIC    - Navigate to desired folder (e.g., `/Workspace/shared/libraries/`)
# MAGIC
# MAGIC 3. **Upload wheel file**:
# MAGIC    - Click **⋮** (three dots menu) → **Upload**
# MAGIC    - Select your `.whl` file from `dist/` directory
# MAGIC    - Click **Upload**
# MAGIC
# MAGIC 4. **Get the workspace path**:
# MAGIC    - After upload, right-click the wheel file
# MAGIC    - Select **Copy file path**
# MAGIC    - Path will look like: `/Workspace/shared/libraries/databricks_course_utils-0.1.0-py3-none-any.whl`
# MAGIC
# MAGIC ## Option 2: Upload via Databricks CLI
# MAGIC
# MAGIC **Prerequisites**: Install and configure Databricks CLI
# MAGIC ```bash
# MAGIC # Install CLI
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Configure authentication
# MAGIC databricks configure --token
# MAGIC # Enter workspace URL and personal access token
# MAGIC ```
# MAGIC
# MAGIC **Upload wheel**:
# MAGIC ```bash
# MAGIC # Upload to workspace
# MAGIC databricks workspace import \
# MAGIC   dist/databricks_course_utils-0.1.0-py3-none-any.whl \
# MAGIC   /Workspace/shared/libraries/databricks_course_utils-0.1.0-py3-none-any.whl \
# MAGIC   --format AUTO
# MAGIC
# MAGIC # Verify upload
# MAGIC databricks workspace list /Workspace/shared/libraries/
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3: Upload to Unity Catalog Volumes (Recommended)
# MAGIC
# MAGIC **Unity Catalog Volumes** provide:
# MAGIC - Better governance and access control
# MAGIC - Integration with Unity Catalog permissions
# MAGIC - Versioning and lineage tracking
# MAGIC - Works seamlessly with Jobs and Workflows
# MAGIC
# MAGIC ### Step 1: Create a Volume (if not exists)
# MAGIC
# MAGIC ```sql
# MAGIC -- Run in Databricks SQL editor or notebook
# MAGIC CREATE VOLUME IF NOT EXISTS databricks_course.shared_bronze.libraries
# MAGIC COMMENT 'Storage for Python wheels and libraries';
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Upload Wheel to Volume
# MAGIC
# MAGIC **Via Databricks UI**:
# MAGIC 1. Navigate to **Catalog Explorer**
# MAGIC 2. Browse to: `databricks_course` → `shared_bronze` → `libraries`
# MAGIC 3. Click **Upload** → Select your `.whl` file
# MAGIC
# MAGIC **Via Databricks CLI**:
# MAGIC ```bash
# MAGIC # Upload to volume
# MAGIC databricks fs cp \
# MAGIC   dist/databricks_course_utils-0.1.0-py3-none-any.whl \
# MAGIC   /Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC
# MAGIC # List files in volume
# MAGIC databricks fs ls /Volumes/databricks_course/shared_bronze/libraries/
# MAGIC ```
# MAGIC
# MAGIC **Via Python (in notebook)**:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Programmatic Upload to Unity Catalog Volume
# MAGIC
# MAGIC If you prefer to automate the upload process using Python, here's how you would do it:
# MAGIC
# MAGIC **Step 1: Create Volume (SQL or Python)**
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS databricks_course.shared_bronze.libraries
# MAGIC COMMENT 'Python wheels and reusable libraries for course';
# MAGIC ```
# MAGIC
# MAGIC Or using PySpark:
# MAGIC ```python
# MAGIC spark.sql("""
# MAGIC CREATE VOLUME IF NOT EXISTS databricks_course.shared_bronze.libraries
# MAGIC COMMENT 'Python wheels and reusable libraries for course'
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC **Step 2: Upload Wheel File**
# MAGIC ```python
# MAGIC # Copy from local file system to Unity Catalog Volume
# MAGIC dbutils.fs.cp(
# MAGIC     "file:///path/to/dist/databricks_course_utils-0.1.0-py3-none-any.whl",
# MAGIC     "/Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Step 3: Verify Upload**
# MAGIC ```python
# MAGIC # List files in the volume
# MAGIC print("📦 Files in libraries volume:")
# MAGIC display(dbutils.fs.ls("/Volumes/databricks_course/shared_bronze/libraries/"))
# MAGIC
# MAGIC # Expected output:
# MAGIC # databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC **When to Use This Approach:**
# MAGIC - Automating deployments in CI/CD pipelines
# MAGIC - Scripting wheel updates across multiple environments
# MAGIC - Integration with version control workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing the Right Upload Method
# MAGIC
# MAGIC | Method | Best For | Pros | Cons |
# MAGIC |--------|----------|------|------|
# MAGIC | **Databricks UI** | - Learning and testing<br>- One-off uploads | - Visual and intuitive<br>- No CLI setup needed | - Manual process<br>- Not automated |
# MAGIC | **Databricks CLI** | - CI/CD pipelines<br>- Automation scripts | - Scriptable<br>- Good for automation | - Requires CLI setup<br>- Less governance |
# MAGIC | **Unity Catalog Volumes** | - Production deployments<br>- Team libraries<br>- Governed environments | - Best governance<br>- Integrated permissions<br>- Version tracking | - Requires Unity Catalog<br>- More setup |
# MAGIC
# MAGIC **Our Recommendation**:
# MAGIC - **For learning (this course)**: Use Databricks UI or Unity Catalog Volumes
# MAGIC - **For production**: Always use Unity Catalog Volumes for governance and traceability

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 7: Using Wheels in Jobs and Notebooks
# MAGIC
# MAGIC Now that the wheel is deployed, here's how you would use it in different contexts.
# MAGIC
# MAGIC > **Note**: These are educational examples showing the patterns. For hands-on practice, see **Notebook 19** for practical job orchestration.
# MAGIC
# MAGIC ## Method 1: Install in Notebook with %pip
# MAGIC
# MAGIC The simplest way to use a wheel in a notebook:
# MAGIC
# MAGIC ```python
# MAGIC # Install wheel from Unity Catalog Volume
# MAGIC %pip install /Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl
# MAGIC
# MAGIC # Restart Python to load the package
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC **After Python restarts, import and use**:
# MAGIC
# MAGIC ```python
# MAGIC # Import from our wheel package
# MAGIC from databricks_course_utils import ingest_csv, remove_duplicates, save_to_delta
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# MAGIC
# MAGIC print("✅ Successfully imported from databricks_course_utils wheel")
# MAGIC print(f"📦 Available functions: ingest_csv, remove_duplicates, save_to_delta")
# MAGIC
# MAGIC # Now you can use the functions
# MAGIC df = ingest_csv(spark, "/path/to/data.csv", schema=my_schema)
# MAGIC df_clean = remove_duplicates(df, subset=["id"])
# MAGIC save_to_delta(df_clean, "catalog.schema.table")
# MAGIC ```
# MAGIC
# MAGIC **When to Use This Method:**
# MAGIC - Interactive notebook development
# MAGIC - Quick prototyping with wheel utilities
# MAGIC - Testing new wheel versions before production deployment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: Configure Cluster Libraries
# MAGIC
# MAGIC For persistent access across all notebooks on a cluster:
# MAGIC
# MAGIC **Step-by-step**:
# MAGIC
# MAGIC 1. **Navigate to Compute**:
# MAGIC    - Go to **Compute** in left sidebar
# MAGIC    - Select your cluster
# MAGIC
# MAGIC 2. **Install library**:
# MAGIC    - Click **Libraries** tab
# MAGIC    - Click **Install new**
# MAGIC    - Select **Library Source**: Python Whl
# MAGIC    - **File Path**: `/Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl`
# MAGIC    - Click **Install**
# MAGIC
# MAGIC 3. **Restart cluster** (if needed):
# MAGIC    - Libraries install automatically on running clusters
# MAGIC    - For immediate use, restart the cluster
# MAGIC
# MAGIC **Pros**:
# MAGIC - No need for `%pip install` in every notebook
# MAGIC - Consistent versions across notebooks
# MAGIC - Good for team clusters
# MAGIC
# MAGIC **Cons**:
# MAGIC - Requires cluster restart for updates
# MAGIC - Affects all notebooks on the cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 3: Configure Job Task Libraries
# MAGIC
# MAGIC For production jobs, specify libraries at the task level:
# MAGIC
# MAGIC **UI Approach**:
# MAGIC 1. Create/edit a Databricks Job
# MAGIC 2. For each task, scroll to **Libraries** section
# MAGIC 3. Click **Add** → **Python Whl**
# MAGIC 4. Enter path: `/Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl`
# MAGIC 5. Save task configuration
# MAGIC
# MAGIC **SDK Approach** (from Notebook 19):
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import jobs
# MAGIC from databricks.sdk.service.jobs import Task, NotebookTask, Source
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Define task with wheel dependency
# MAGIC task = Task(
# MAGIC     task_key="etl_with_utils",
# MAGIC     description="ETL task using our wheel utilities",
# MAGIC     notebook_task=NotebookTask(
# MAGIC         notebook_path="/Workspace/course/notebooks/02_week/06_file_ingestion",
# MAGIC         source=Source.WORKSPACE
# MAGIC     ),
# MAGIC     libraries=[
# MAGIC         jobs.Library(
# MAGIC             whl="/Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl"
# MAGIC         )
# MAGIC     ],
# MAGIC     new_cluster=jobs.ClusterSpec(
# MAGIC         spark_version="14.3.x-scala2.12",
# MAGIC         node_type_id="i3.xlarge",
# MAGIC         num_workers=2
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC # Create job with library dependency
# MAGIC created_job = w.jobs.create(
# MAGIC     name="ETL Job with Wheel Utils",
# MAGIC     tasks=[task]
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: Complete Pipeline Using Wheel Utilities
# MAGIC
# MAGIC Here's what a complete medallion architecture pipeline would look like using wheel utilities:
# MAGIC
# MAGIC ### Setup and Installation
# MAGIC
# MAGIC ```python
# MAGIC # Set up user schema
# MAGIC %run ../utils/user_schema_setup
# MAGIC
# MAGIC # Install wheel from Unity Catalog Volume
# MAGIC %pip install /Volumes/databricks_course/shared_bronze/libraries/databricks_course_utils-0.1.0-py3-none-any.whl --quiet
# MAGIC dbutils.library.restartPython()
# MAGIC
# MAGIC # Import wheel utilities
# MAGIC from databricks_course_utils import (
# MAGIC     ingest_csv,
# MAGIC     standardize_string_columns,
# MAGIC     remove_duplicates,
# MAGIC     fill_missing_values,
# MAGIC     validate_data_quality,
# MAGIC     save_to_delta
# MAGIC )
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# MAGIC from pyspark.sql.functions import col, current_timestamp
# MAGIC
# MAGIC print("✅ Wheel utilities loaded successfully")
# MAGIC ```
# MAGIC
# MAGIC ### Bronze Layer: Data Ingestion
# MAGIC
# MAGIC ```python
# MAGIC # Define schema for customer data
# MAGIC customer_schema = StructType([
# MAGIC     StructField("customer_id", IntegerType(), False),
# MAGIC     StructField("customer_name", StringType(), True),
# MAGIC     StructField("email", StringType(), True),
# MAGIC     StructField("country", StringType(), True),
# MAGIC     StructField("total_purchases", DoubleType(), True)
# MAGIC ])
# MAGIC
# MAGIC # Ingest CSV file using wheel utility
# MAGIC df_bronze = ingest_csv(
# MAGIC     spark,
# MAGIC     file_path="/Volumes/databricks_course/shared_bronze/data/customers.csv",
# MAGIC     schema=customer_schema
# MAGIC )
# MAGIC
# MAGIC # Save to Bronze layer
# MAGIC bronze_table = get_table_path("bronze", "customers_raw")
# MAGIC save_to_delta(df_bronze, bronze_table, mode="overwrite")
# MAGIC
# MAGIC print(f"✅ Bronze layer saved: {bronze_table}")
# MAGIC print(f"   Records: {df_bronze.count()}")
# MAGIC ```
# MAGIC
# MAGIC ### Silver Layer: Data Cleaning and Transformation
# MAGIC
# MAGIC ```python
# MAGIC # Read from Bronze layer
# MAGIC df_silver = spark.table(bronze_table)
# MAGIC
# MAGIC # Step 1: Remove duplicates using wheel utility
# MAGIC df_silver = remove_duplicates(df_silver, subset=["customer_id"])
# MAGIC print(f"After deduplication: {df_silver.count()} records")
# MAGIC
# MAGIC # Step 2: Standardize string columns using wheel utility
# MAGIC df_silver = standardize_string_columns(
# MAGIC     df_silver,
# MAGIC     columns=["customer_name", "country"],
# MAGIC     case="upper",
# MAGIC     trim_spaces=True
# MAGIC )
# MAGIC print("✅ Standardized name and country columns")
# MAGIC
# MAGIC # Step 3: Fill missing values using wheel utility
# MAGIC df_silver = fill_missing_values(df_silver, column="email", fill_value="unknown@example.com")
# MAGIC df_silver = fill_missing_values(df_silver, column="country", fill_value="UNKNOWN")
# MAGIC df_silver = fill_missing_values(df_silver, column="total_purchases", fill_value=0.0)
# MAGIC print("✅ Filled missing values")
# MAGIC
# MAGIC # Step 4: Add metadata
# MAGIC df_silver = df_silver.withColumn("processed_at", current_timestamp())
# MAGIC
# MAGIC # Save to Silver layer
# MAGIC silver_table = get_table_path("silver", "customers_cleaned")
# MAGIC save_to_delta(df_silver, silver_table, mode="overwrite")
# MAGIC
# MAGIC print(f"✅ Silver layer saved: {silver_table}")
# MAGIC ```
# MAGIC
# MAGIC ### Gold Layer: Data Validation and Quality
# MAGIC
# MAGIC ```python
# MAGIC # Read from Silver layer
# MAGIC df_gold = spark.table(silver_table)
# MAGIC
# MAGIC # Validate data quality using wheel utility
# MAGIC good_records, bad_records = validate_data_quality(
# MAGIC     df_gold,
# MAGIC     required_columns=["customer_id", "customer_name", "email"],
# MAGIC     non_null_columns=["customer_id", "customer_name"]
# MAGIC )
# MAGIC
# MAGIC print(f"Good records: {good_records.count()}")
# MAGIC print(f"Bad records: {bad_records.count()}")
# MAGIC
# MAGIC # Save good records to Gold layer
# MAGIC gold_table = get_table_path("gold", "customers_validated")
# MAGIC save_to_delta(good_records, gold_table, mode="overwrite")
# MAGIC
# MAGIC # Save bad records to quarantine (if any)
# MAGIC if bad_records.count() > 0:
# MAGIC     quarantine_table = get_table_path("bronze", "customers_quarantine")
# MAGIC     save_to_delta(bad_records, quarantine_table, mode="overwrite")
# MAGIC     print(f"⚠️ Quarantined {bad_records.count()} records: {quarantine_table}")
# MAGIC
# MAGIC print(f"✅ Gold layer saved: {gold_table}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Pipeline Demonstrates
# MAGIC
# MAGIC Using the `databricks_course_utils` wheel, this pipeline achieves:
# MAGIC
# MAGIC 1. ✅ **Bronze Layer** - Raw data ingestion with schema validation
# MAGIC    - `ingest_csv()` - Type-safe CSV ingestion
# MAGIC    - `save_to_delta()` - Standardized Delta table writes
# MAGIC
# MAGIC 2. ✅ **Silver Layer** - Data cleaning and standardization
# MAGIC    - `remove_duplicates()` - Deduplication by key columns
# MAGIC    - `standardize_string_columns()` - Consistent text formatting
# MAGIC    - `fill_missing_values()` - Handle nulls with defaults
# MAGIC
# MAGIC 3. ✅ **Gold Layer** - Data quality validation
# MAGIC    - `validate_data_quality()` - Separate good/bad records
# MAGIC    - Quarantine pattern for bad records
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Benefits of This Approach
# MAGIC
# MAGIC **Reusability**:
# MAGIC - Same utilities work across all notebooks and jobs
# MAGIC - No code duplication - import and use
# MAGIC
# MAGIC **Testability**:
# MAGIC - Functions are unit tested before deployment
# MAGIC - Confident code changes with test coverage
# MAGIC
# MAGIC **Maintainability**:
# MAGIC - Update once in wheel, all jobs benefit immediately
# MAGIC - Centralized business logic
# MAGIC
# MAGIC **Version Control**:
# MAGIC - Track changes with semantic versioning
# MAGIC - Easy rollback to previous versions
# MAGIC
# MAGIC **Team Collaboration**:
# MAGIC - Everyone uses same tested utilities
# MAGIC - Consistent data patterns across projects

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Best Practices
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Wheels are for production**: Use notebooks for exploration, wheels for production jobs
# MAGIC 2. **Poetry simplifies packaging**: Handles dependencies, building, and versioning elegantly
# MAGIC 3. **Unity Catalog Volumes are best**: Modern, governed storage for libraries
# MAGIC 4. **Version your wheels**: Use semantic versioning for clear change tracking
# MAGIC 5. **Test before deploying**: Always validate wheels locally before Databricks upload
# MAGIC
# MAGIC ## Development Workflow
# MAGIC
# MAGIC ```
# MAGIC 1. Develop → Write code in src/
# MAGIC 2. Test    → Run pytest tests/
# MAGIC 3. Build   → poetry build
# MAGIC 4. Upload  → Deploy to Unity Catalog Volume
# MAGIC 5. Use     → Install in notebooks or configure in jobs
# MAGIC 6. Iterate → Update version, rebuild, redeploy
# MAGIC ```
# MAGIC
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### ✅ DO
# MAGIC - Use Poetry for dependency management
# MAGIC - Write unit tests for all utility functions
# MAGIC - Use semantic versioning (MAJOR.MINOR.PATCH)
# MAGIC - Deploy to Unity Catalog Volumes for production
# MAGIC - Document your functions with docstrings
# MAGIC - Keep wheels focused (one purpose per wheel)
# MAGIC - Pin PySpark version to match Databricks runtime
# MAGIC
# MAGIC ### ❌ DON'T
# MAGIC - Don't include Databricks-specific code in wheels (keep it runtime-agnostic)
# MAGIC - Don't hardcode paths or credentials in wheel code
# MAGIC - Don't skip testing before deployment
# MAGIC - Don't use different PySpark versions than your runtime
# MAGIC - Don't mix business logic with orchestration logic
# MAGIC
# MAGIC ## When to Update Your Wheel
# MAGIC
# MAGIC | Scenario | Version Bump | Example |
# MAGIC |----------|--------------|---------|
# MAGIC | Bug fix in existing function | PATCH | 0.1.0 → 0.1.1 |
# MAGIC | New function added (backwards compatible) | MINOR | 0.1.1 → 0.2.0 |
# MAGIC | Function signature changed (breaking) | MAJOR | 0.2.0 → 1.0.0 |
# MAGIC | Documentation update only | PATCH | 1.0.0 → 1.0.1 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In **Notebook 21**, we'll take everything we've learned and build a real production pipeline:
# MAGIC - Use **stock market data** from an open dataset
# MAGIC - Create a comprehensive wheel with ingestion and transformation utilities
# MAGIC - Deploy the wheel to Unity Catalog Volumes
# MAGIC - Build a multi-task job using both UI and SDK approaches
# MAGIC - Monitor and validate the production pipeline
# MAGIC
# MAGIC You'll see how wheels scale from simple utilities (Notebook 20) to complete production systems (Notebook 21).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Resources**:
# MAGIC - [Poetry Documentation](https://python-poetry.org/docs/)
# MAGIC - [Databricks Libraries Guide](https://docs.databricks.com/libraries/index.html)
# MAGIC - [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
# MAGIC - [Python Packaging Guide](https://packaging.python.org/)