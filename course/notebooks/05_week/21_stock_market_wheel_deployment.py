# Databricks notebook source
# MAGIC %md
# MAGIC # Week 5 - Notebook 21: Production Stock Market Pipeline with Wheel Deployment
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this notebook, you will be able to:
# MAGIC - Build a production-ready Python wheel for financial data processing
# MAGIC - Ingest real stock market data from public APIs
# MAGIC - Implement medallion architecture with real-world data
# MAGIC - Deploy wheels and orchestrate multi-task jobs
# MAGIC - Monitor and validate production data pipelines
# MAGIC - Use both UI and SDK approaches for complete workflow automation
# MAGIC
# MAGIC ## Project Overview
# MAGIC
# MAGIC This notebook demonstrates a **complete production workflow**:
# MAGIC
# MAGIC ```
# MAGIC 1. BUILD WHEEL
# MAGIC    ├── Stock market ingestion utilities (Alpha Vantage API)
# MAGIC    ├── Financial data transformations (returns, volatility)
# MAGIC    └── Validation and quality checks
# MAGIC
# MAGIC 2. DEPLOY WHEEL
# MAGIC    ├── Upload to Unity Catalog Volume
# MAGIC    └── Version management
# MAGIC
# MAGIC 3. CREATE PIPELINE
# MAGIC    ├── Bronze: Ingest raw stock data
# MAGIC    ├── Silver: Calculate returns and metrics
# MAGIC    └── Gold: Aggregate market insights
# MAGIC
# MAGIC 4. ORCHESTRATE JOB
# MAGIC    ├── UI Approach: Step-by-step guide
# MAGIC    └── SDK Approach: Programmatic automation
# MAGIC ```
# MAGIC
# MAGIC ## Real-World Data Source
# MAGIC
# MAGIC We'll use **yfinance** (Yahoo Finance API) for free, reliable stock market data:
# MAGIC - No API key required
# MAGIC - Historical and real-time data
# MAGIC - Multiple stocks and indices
# MAGIC - Industry-standard financial data
# MAGIC
# MAGIC ## Notebook Structure
# MAGIC - **Part 1**: Wheel Package Development
# MAGIC - **Part 2**: Stock Market Data Pipeline
# MAGIC - **Part 3**: Wheel Deployment to Databricks
# MAGIC - **Part 4**: Job Orchestration (UI Approach)
# MAGIC - **Part 5**: Job Orchestration (SDK Approach)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Building the Stock Market Wheel Package
# MAGIC
# MAGIC ## Package Structure
# MAGIC
# MAGIC Our production wheel package (`stock_market_utils`) will have this structure:
# MAGIC
# MAGIC ```
# MAGIC stock-market-utils/
# MAGIC ├── src/
# MAGIC │   └── stock_market_utils/
# MAGIC │       ├── __init__.py
# MAGIC │       ├── ingestion/
# MAGIC │       │   ├── __init__.py
# MAGIC │       │   └── yahoo_finance.py      # yfinance integration
# MAGIC │       ├── transformations/
# MAGIC │       │   ├── __init__.py
# MAGIC │       │   ├── returns.py            # Return calculations
# MAGIC │       │   ├── volatility.py         # Risk metrics
# MAGIC │       │   └── indicators.py         # Technical indicators
# MAGIC │       └── utils/
# MAGIC │           ├── __init__.py
# MAGIC │           └── validators.py         # Data quality checks
# MAGIC ├── tests/
# MAGIC │   ├── test_ingestion.py
# MAGIC │   ├── test_transformations.py
# MAGIC │   └── test_validators.py
# MAGIC ├── pyproject.toml
# MAGIC └── README.md
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## pyproject.toml Configuration
# MAGIC
# MAGIC **File**: `pyproject.toml`
# MAGIC
# MAGIC ```toml
# MAGIC [tool.poetry]
# MAGIC name = "stock-market-utils"
# MAGIC version = "1.0.0"
# MAGIC description = "Production utilities for stock market data processing on Databricks"
# MAGIC authors = ["Data Engineering Team <team@example.com>"]
# MAGIC readme = "README.md"
# MAGIC packages = [{include = "stock_market_utils", from = "src"}]
# MAGIC
# MAGIC [tool.poetry.dependencies]
# MAGIC python = "^3.11"
# MAGIC pyspark = "^3.5.0"
# MAGIC delta-spark = "^3.0.0"
# MAGIC yfinance = "^0.2.36"      # Yahoo Finance API client
# MAGIC pandas = "^2.1.0"         # Data manipulation
# MAGIC numpy = "^1.26.0"         # Numerical computations
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 1: Yahoo Finance Ingestion
# MAGIC
# MAGIC **File**: `src/stock_market_utils/ingestion/yahoo_finance.py`
# MAGIC
# MAGIC ```python
# MAGIC """Yahoo Finance data ingestion utilities."""
# MAGIC
# MAGIC import yfinance as yf
# MAGIC import pandas as pd
# MAGIC from datetime import datetime, timedelta
# MAGIC from typing import List, Optional
# MAGIC from pyspark.sql import SparkSession, DataFrame
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
# MAGIC
# MAGIC
# MAGIC def get_stock_schema() -> StructType:
# MAGIC     """Return schema for stock market data."""
# MAGIC     return StructType([
# MAGIC         StructField("symbol", StringType(), False),
# MAGIC         StructField("date", TimestampType(), False),
# MAGIC         StructField("open", DoubleType(), True),
# MAGIC         StructField("high", DoubleType(), True),
# MAGIC         StructField("low", DoubleType(), True),
# MAGIC         StructField("close", DoubleType(), True),
# MAGIC         StructField("volume", LongType(), True),
# MAGIC         StructField("dividends", DoubleType(), True),
# MAGIC         StructField("stock_splits", DoubleType(), True),
# MAGIC     ])
# MAGIC
# MAGIC
# MAGIC def fetch_stock_data(
# MAGIC     symbols: List[str],
# MAGIC     start_date: str,
# MAGIC     end_date: str,
# MAGIC     interval: str = "1d"
# MAGIC ) -> pd.DataFrame:
# MAGIC     """
# MAGIC     Fetch historical stock data from Yahoo Finance.
# MAGIC
# MAGIC     Args:
# MAGIC         symbols: List of stock symbols (e.g., ["AAPL", "GOOGL", "MSFT"])
# MAGIC         start_date: Start date in YYYY-MM-DD format
# MAGIC         end_date: End date in YYYY-MM-DD format
# MAGIC         interval: Data interval (1d, 1h, etc.)
# MAGIC
# MAGIC     Returns:
# MAGIC         Pandas DataFrame with stock data
# MAGIC
# MAGIC     Example:
# MAGIC         >>> data = fetch_stock_data(
# MAGIC         ...     symbols=["AAPL", "GOOGL"],
# MAGIC         ...     start_date="2024-01-01",
# MAGIC         ...     end_date="2024-12-31"
# MAGIC         ... )
# MAGIC     """
# MAGIC     all_data = []
# MAGIC
# MAGIC     for symbol in symbols:
# MAGIC         try:
# MAGIC             ticker = yf.Ticker(symbol)
# MAGIC             hist = ticker.history(start=start_date, end=end_date, interval=interval)
# MAGIC
# MAGIC             if hist.empty:
# MAGIC                 print(f"⚠️  No data found for {symbol}")
# MAGIC                 continue
# MAGIC
# MAGIC             # Reset index to get date as column
# MAGIC             hist = hist.reset_index()
# MAGIC
# MAGIC             # Add symbol column
# MAGIC             hist["symbol"] = symbol
# MAGIC
# MAGIC             # Rename columns to match schema
# MAGIC             hist = hist.rename(columns={
# MAGIC                 "Date": "date",
# MAGIC                 "Open": "open",
# MAGIC                 "High": "high",
# MAGIC                 "Low": "low",
# MAGIC                 "Close": "close",
# MAGIC                 "Volume": "volume",
# MAGIC                 "Dividends": "dividends",
# MAGIC                 "Stock Splits": "stock_splits"
# MAGIC             })
# MAGIC
# MAGIC             # Select relevant columns
# MAGIC             hist = hist[["symbol", "date", "open", "high", "low", "close", "volume", "dividends", "stock_splits"]]
# MAGIC
# MAGIC             all_data.append(hist)
# MAGIC             print(f"✅ Fetched {len(hist)} records for {symbol}")
# MAGIC
# MAGIC         except Exception as e:
# MAGIC             print(f"❌ Error fetching {symbol}: {str(e)}")
# MAGIC             continue
# MAGIC
# MAGIC     if not all_data:
# MAGIC         raise ValueError("No data fetched for any symbols")
# MAGIC
# MAGIC     # Combine all data
# MAGIC     combined_data = pd.concat(all_data, ignore_index=True)
# MAGIC     return combined_data
# MAGIC
# MAGIC
# MAGIC def ingest_stock_data_to_spark(
# MAGIC     spark: SparkSession,
# MAGIC     symbols: List[str],
# MAGIC     start_date: str,
# MAGIC     end_date: str,
# MAGIC     interval: str = "1d"
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Fetch stock data and convert to Spark DataFrame.
# MAGIC
# MAGIC     Args:
# MAGIC         spark: SparkSession instance
# MAGIC         symbols: List of stock symbols
# MAGIC         start_date: Start date (YYYY-MM-DD)
# MAGIC         end_date: End date (YYYY-MM-DD)
# MAGIC         interval: Data interval
# MAGIC
# MAGIC     Returns:
# MAGIC         Spark DataFrame with stock data
# MAGIC     """
# MAGIC     # Fetch data as pandas
# MAGIC     pandas_df = fetch_stock_data(symbols, start_date, end_date, interval)
# MAGIC
# MAGIC     # Convert to Spark DataFrame with explicit schema
# MAGIC     schema = get_stock_schema()
# MAGIC     spark_df = spark.createDataFrame(pandas_df, schema=schema)
# MAGIC
# MAGIC     return spark_df
# MAGIC
# MAGIC
# MAGIC def save_to_bronze(
# MAGIC     df: DataFrame,
# MAGIC     table_name: str,
# MAGIC     partition_by: Optional[List[str]] = None
# MAGIC ) -> None:
# MAGIC     """Save DataFrame to Bronze Delta table."""
# MAGIC     writer = df.write.format("delta").mode("overwrite")
# MAGIC
# MAGIC     if partition_by:
# MAGIC         writer = writer.partitionBy(*partition_by)
# MAGIC
# MAGIC     writer.saveAsTable(table_name)
# MAGIC     print(f"✅ Saved {df.count()} records to {table_name}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 2: Financial Transformations - Returns
# MAGIC
# MAGIC **File**: `src/stock_market_utils/transformations/returns.py`
# MAGIC
# MAGIC ```python
# MAGIC """Financial return calculations."""
# MAGIC
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import col, lag, round as spark_round, first
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC
# MAGIC def calculate_daily_returns(df: DataFrame) -> DataFrame:
# MAGIC    """
# MAGIC    Calculate daily returns for each stock.
# MAGIC
# MAGIC    Formula: (Close_today - Close_yesterday) / Close_yesterday
# MAGIC
# MAGIC    Args:
# MAGIC        df: DataFrame with columns [symbol, date, close]
# MAGIC
# MAGIC    Returns:
# MAGIC        DataFrame with additional daily_return column
# MAGIC    """
# MAGIC    window_spec = Window.partitionBy("symbol").orderBy("date")
# MAGIC
# MAGIC    df_with_returns = df.withColumn(
# MAGIC        "previous_close",
# MAGIC        lag("close", 1).over(window_spec)
# MAGIC    )
# MAGIC
# MAGIC    df_with_returns = df_with_returns.withColumn(
# MAGIC        "daily_return",
# MAGIC        spark_round(
# MAGIC            ((col("close") - col("previous_close")) / col("previous_close")) * 100,
# MAGIC            4
# MAGIC        )
# MAGIC    )
# MAGIC
# MAGIC    # Drop intermediate column
# MAGIC    df_with_returns = df_with_returns.drop("previous_close")
# MAGIC
# MAGIC    return df_with_returns
# MAGIC
# MAGIC
# MAGIC def calculate_cumulative_returns(df: DataFrame) -> DataFrame:
# MAGIC    """
# MAGIC    Calculate cumulative returns from start date.
# MAGIC
# MAGIC    Args:
# MAGIC        df: DataFrame with columns [symbol, date, close]
# MAGIC
# MAGIC    Returns:
# MAGIC        DataFrame with cumulative_return column
# MAGIC    """
# MAGIC    window_spec = Window.partitionBy("symbol").orderBy("date")
# MAGIC
# MAGIC    # Get first close price for each symbol
# MAGIC    df_with_first = df.withColumn(
# MAGIC        "first_close",
# MAGIC        first("close").over(window_spec)
# MAGIC    )
# MAGIC
# MAGIC    # Calculate cumulative return
# MAGIC    df_with_cumulative = df_with_first.withColumn(
# MAGIC        "cumulative_return",
# MAGIC        spark_round(
# MAGIC            ((col("close") - col("first_close")) / col("first_close")) * 100,
# MAGIC            4
# MAGIC        )
# MAGIC    )
# MAGIC
# MAGIC    df_with_cumulative = df_with_cumulative.drop("first_close")
# MAGIC
# MAGIC    return df_with_cumulative
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 3: Volatility Metrics
# MAGIC
# MAGIC **File**: `src/stock_market_utils/transformations/volatility.py`
# MAGIC
# MAGIC ```python
# MAGIC """Volatility and risk metrics."""
# MAGIC
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import col, stddev, avg, max as spark_max, min as spark_min, round as spark_round
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC
# MAGIC def calculate_rolling_volatility(
# MAGIC     df: DataFrame,
# MAGIC     window_days: int = 30,
# MAGIC     column: str = "daily_return"
# MAGIC ) -> DataFrame:
# MAGIC     """
# MAGIC     Calculate rolling volatility (standard deviation of returns).
# MAGIC
# MAGIC     Args:
# MAGIC         df: DataFrame with daily_return column
# MAGIC         window_days: Rolling window size in days
# MAGIC         column: Column to calculate volatility on
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame with rolling_volatility column
# MAGIC     """
# MAGIC     window_spec = (
# MAGIC         Window
# MAGIC         .partitionBy("symbol")
# MAGIC         .orderBy("date")
# MAGIC         .rowsBetween(-(window_days - 1), 0)
# MAGIC     )
# MAGIC
# MAGIC     df_with_volatility = df.withColumn(
# MAGIC         f"rolling_volatility_{window_days}d",
# MAGIC         spark_round(stddev(col(column)).over(window_spec), 4)
# MAGIC     )
# MAGIC
# MAGIC     return df_with_volatility
# MAGIC
# MAGIC
# MAGIC def calculate_price_range_metrics(df: DataFrame) -> DataFrame:
# MAGIC     """
# MAGIC     Calculate intraday price range metrics.
# MAGIC
# MAGIC     Args:
# MAGIC         df: DataFrame with high, low, close columns
# MAGIC
# MAGIC     Returns:
# MAGIC         DataFrame with price range metrics
# MAGIC     """
# MAGIC     df_with_range = df.withColumn(
# MAGIC         "daily_range",
# MAGIC         spark_round(col("high") - col("low"), 2)
# MAGIC     )
# MAGIC
# MAGIC     df_with_range = df_with_range.withColumn(
# MAGIC         "daily_range_pct",
# MAGIC         spark_round(((col("high") - col("low")) / col("close")) * 100, 4)
# MAGIC     )
# MAGIC
# MAGIC     return df_with_range
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 4: Data Validators
# MAGIC
# MAGIC **File**: `src/stock_market_utils/utils/validators.py`
# MAGIC
# MAGIC ```python
# MAGIC """Data quality validation for stock market data."""
# MAGIC
# MAGIC from pyspark.sql import DataFrame
# MAGIC from pyspark.sql.functions import col, count, when, isnan, isnull
# MAGIC
# MAGIC
# MAGIC def validate_stock_data(df: DataFrame) -> dict:
# MAGIC     """
# MAGIC     Validate stock data quality.
# MAGIC
# MAGIC     Args:
# MAGIC         df: Stock data DataFrame
# MAGIC
# MAGIC     Returns:
# MAGIC         Dictionary with validation results
# MAGIC     """
# MAGIC     total_records = df.count()
# MAGIC
# MAGIC     # Check for nulls in critical columns
# MAGIC     critical_columns = ["symbol", "date", "close"]
# MAGIC     null_counts = {}
# MAGIC
# MAGIC     for column in critical_columns:
# MAGIC         null_count = df.filter(col(column).isNull()).count()
# MAGIC         null_counts[column] = null_count
# MAGIC
# MAGIC     # Check for negative prices
# MAGIC     price_columns = ["open", "high", "low", "close"]
# MAGIC     negative_prices = df.filter(
# MAGIC         (col("open") < 0) | (col("high") < 0) | (col("low") < 0) | (col("close") < 0)
# MAGIC     ).count()
# MAGIC
# MAGIC     # Check for invalid high/low relationships
# MAGIC     invalid_ranges = df.filter(col("high") < col("low")).count()
# MAGIC
# MAGIC     # Check for duplicate records
# MAGIC     duplicates = df.groupBy("symbol", "date").count().filter(col("count") > 1).count()
# MAGIC
# MAGIC     validation_results = {
# MAGIC         "total_records": total_records,
# MAGIC         "null_counts": null_counts,
# MAGIC         "negative_prices": negative_prices,
# MAGIC         "invalid_ranges": invalid_ranges,
# MAGIC         "duplicates": duplicates,
# MAGIC         "is_valid": (
# MAGIC             sum(null_counts.values()) == 0
# MAGIC             and negative_prices == 0
# MAGIC             and invalid_ranges == 0
# MAGIC             and duplicates == 0
# MAGIC         )
# MAGIC     }
# MAGIC
# MAGIC     return validation_results
# MAGIC
# MAGIC
# MAGIC def print_validation_report(validation_results: dict) -> None:
# MAGIC     """Print formatted validation report."""
# MAGIC     print("=" * 60)
# MAGIC     print("📊 STOCK DATA VALIDATION REPORT")
# MAGIC     print("=" * 60)
# MAGIC     print(f"Total Records: {validation_results['total_records']:,}")
# MAGIC     print()
# MAGIC     print("Null Values:")
# MAGIC     for col, count in validation_results['null_counts'].items():
# MAGIC         status = "✅" if count == 0 else "❌"
# MAGIC         print(f"  {status} {col}: {count}")
# MAGIC     print()
# MAGIC     print(f"Negative Prices: {validation_results['negative_prices']}")
# MAGIC     print(f"Invalid Ranges (high < low): {validation_results['invalid_ranges']}")
# MAGIC     print(f"Duplicate Records: {validation_results['duplicates']}")
# MAGIC     print()
# MAGIC     if validation_results['is_valid']:
# MAGIC         print("✅ VALIDATION PASSED - Data is ready for processing")
# MAGIC     else:
# MAGIC         print("❌ VALIDATION FAILED - Please review data quality issues")
# MAGIC     print("=" * 60)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Package Initialization
# MAGIC
# MAGIC **File**: `src/stock_market_utils/__init__.py`
# MAGIC
# MAGIC ```python
# MAGIC """Stock Market Utilities for Databricks."""
# MAGIC
# MAGIC __version__ = "1.0.0"
# MAGIC
# MAGIC # Ingestion
# MAGIC from stock_market_utils.ingestion.yahoo_finance import (
# MAGIC     fetch_stock_data,
# MAGIC     ingest_stock_data_to_spark,
# MAGIC     save_to_bronze,
# MAGIC     get_stock_schema
# MAGIC )
# MAGIC
# MAGIC # Transformations
# MAGIC from stock_market_utils.transformations.returns import (
# MAGIC     calculate_daily_returns,
# MAGIC     calculate_cumulative_returns
# MAGIC )
# MAGIC
# MAGIC from stock_market_utils.transformations.volatility import (
# MAGIC     calculate_rolling_volatility,
# MAGIC     calculate_price_range_metrics
# MAGIC )
# MAGIC
# MAGIC # Validators
# MAGIC from stock_market_utils.utils.validators import (
# MAGIC     validate_stock_data,
# MAGIC     print_validation_report
# MAGIC )
# MAGIC
# MAGIC __all__ = [
# MAGIC     # Ingestion
# MAGIC     "fetch_stock_data",
# MAGIC     "ingest_stock_data_to_spark",
# MAGIC     "save_to_bronze",
# MAGIC     "get_stock_schema",
# MAGIC     # Transformations
# MAGIC     "calculate_daily_returns",
# MAGIC     "calculate_cumulative_returns",
# MAGIC     "calculate_rolling_volatility",
# MAGIC     "calculate_price_range_metrics",
# MAGIC     # Validators
# MAGIC     "validate_stock_data",
# MAGIC     "print_validation_report",
# MAGIC ]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the Wheel
# MAGIC
# MAGIC After creating all the modules above, build the wheel locally:
# MAGIC
# MAGIC ```bash
# MAGIC # Navigate to project directory
# MAGIC cd stock-market-utils
# MAGIC
# MAGIC # Install dependencies
# MAGIC poetry install
# MAGIC
# MAGIC # Run tests (if you created them)
# MAGIC poetry run pytest tests/ -v
# MAGIC
# MAGIC # Build the wheel
# MAGIC poetry build
# MAGIC
# MAGIC # Output:
# MAGIC # Building stock-market-utils (1.0.0)
# MAGIC #   - Building sdist
# MAGIC #   - Built stock_market_utils-1.0.0.tar.gz
# MAGIC #   - Building wheel
# MAGIC #   - Built stock_market_utils-1.0.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC **Result**: `dist/stock_market_utils-1.0.0-py3-none-any.whl`

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Pipeline Implementation - Reference Examples
# MAGIC
# MAGIC This section shows **what the production pipeline code would look like** if you were to implement it.
# MAGIC
# MAGIC > **Note**: The code below is for educational reference only. For hands-on pipeline development, you can run this interactively or refer to the earlier notebooks in Week 2-4.
# MAGIC
# MAGIC ## Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC Bronze Layer (Raw Data)
# MAGIC   ├── Ingest from Yahoo Finance API using wheel utilities
# MAGIC   ├── Store raw OHLCV data in Delta tables
# MAGIC   └── Validate data quality with wheel validators
# MAGIC       ↓
# MAGIC Silver Layer (Cleaned & Enriched)
# MAGIC   ├── Calculate daily returns using wheel transformations
# MAGIC   ├── Calculate cumulative returns
# MAGIC   ├── Add price range metrics
# MAGIC   └── Remove duplicates and nulls
# MAGIC       ↓
# MAGIC Gold Layer (Analytics-Ready)
# MAGIC   ├── Aggregate by symbol
# MAGIC   ├── Calculate volatility metrics using wheel utilities
# MAGIC   ├── Compute performance statistics
# MAGIC   └── Create market summary for business stakeholders
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Bronze Layer Implementation Example
# MAGIC
# MAGIC **Notebook**: `bronze_ingestion.py` (separate notebook for production)
# MAGIC
# MAGIC ```python
# MAGIC # COMMAND ----------
# MAGIC # Import wheel utilities
# MAGIC from stock_market_utils import (
# MAGIC     ingest_stock_data_to_spark,
# MAGIC     save_to_bronze,
# MAGIC     validate_stock_data,
# MAGIC     print_validation_report
# MAGIC )
# MAGIC
# MAGIC # Get parameters from job configuration
# MAGIC dbutils.widgets.text("symbols", "AAPL,GOOGL,MSFT", "Stock Symbols")
# MAGIC dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
# MAGIC dbutils.widgets.text("end_date", "2024-12-31", "End Date")
# MAGIC
# MAGIC symbols = dbutils.widgets.get("symbols").split(",")
# MAGIC start_date = dbutils.widgets.get("start_date")
# MAGIC end_date = dbutils.widgets.get("end_date")
# MAGIC
# MAGIC print(f"📈 Ingesting stock data for: {', '.join(symbols)}")
# MAGIC print(f"📅 Date range: {start_date} to {end_date}")
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Ingest data using wheel utility
# MAGIC df_bronze = ingest_stock_data_to_spark(
# MAGIC     spark=spark,
# MAGIC     symbols=symbols,
# MAGIC     start_date=start_date,
# MAGIC     end_date=end_date
# MAGIC )
# MAGIC
# MAGIC print(f"✅ Fetched {df_bronze.count():,} records")
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Validate data quality using wheel validator
# MAGIC validation_results = validate_stock_data(df_bronze)
# MAGIC print_validation_report(validation_results)
# MAGIC
# MAGIC if not validation_results['is_valid']:
# MAGIC     raise ValueError("Data quality validation failed!")
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Save to Bronze Delta table
# MAGIC bronze_table = "databricks_course.user_schema.bronze_stock_market_raw"
# MAGIC save_to_bronze(df_bronze, bronze_table)
# MAGIC
# MAGIC print(f"✅ Bronze layer saved: {bronze_table}")
# MAGIC display(df_bronze.limit(10))
# MAGIC ```
# MAGIC
# MAGIC **Key Benefits of Using Wheel**:
# MAGIC - ✅ Clean, readable code (logic encapsulated in wheel functions)
# MAGIC - ✅ Reusable across multiple pipelines
# MAGIC - ✅ Testable (wheel functions have unit tests)
# MAGIC - ✅ Easy to maintain (update wheel version, not every notebook)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Layer Implementation Example
# MAGIC
# MAGIC **Notebook**: `silver_transformations.py` (separate notebook for production)
# MAGIC
# MAGIC ```python
# MAGIC # COMMAND ----------
# MAGIC # Import wheel transformation utilities
# MAGIC from stock_market_utils import (
# MAGIC     calculate_daily_returns,
# MAGIC     calculate_cumulative_returns,
# MAGIC     calculate_price_range_metrics
# MAGIC )
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Read from Bronze layer
# MAGIC bronze_table = "databricks_course.user_schema.bronze_stock_market_raw"
# MAGIC df_silver = spark.table(bronze_table)
# MAGIC
# MAGIC print(f"📖 Read {df_silver.count():,} records from Bronze")
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Apply transformations using wheel utilities
# MAGIC df_silver = calculate_daily_returns(df_silver)
# MAGIC df_silver = calculate_cumulative_returns(df_silver)
# MAGIC df_silver = calculate_price_range_metrics(df_silver)
# MAGIC
# MAGIC # Add processing metadata
# MAGIC df_silver = df_silver.withColumn("processed_at", current_timestamp())
# MAGIC
# MAGIC print("✅ Applied transformations:")
# MAGIC print("   - Daily returns")
# MAGIC print("   - Cumulative returns")
# MAGIC print("   - Price range metrics")
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Save to Silver Delta table
# MAGIC silver_table = "databricks_course.user_schema.silver_stock_market_returns"
# MAGIC
# MAGIC df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)
# MAGIC
# MAGIC print(f"✅ Silver layer saved: {silver_table}")
# MAGIC display(df_silver.select("symbol", "date", "close", "daily_return", "cumulative_return").limit(10))
# MAGIC ```
# MAGIC
# MAGIC **Transformation Logic**:
# MAGIC - All calculation logic lives in the wheel package
# MAGIC - Notebook focuses on orchestration (read → transform → write)
# MAGIC - Easy to test transformations independently
# MAGIC - Consistent business logic across all pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Gold Layer Implementation Example
# MAGIC
# MAGIC **Notebook**: `gold_analytics.py` (separate notebook for production)
# MAGIC
# MAGIC ```python
# MAGIC # COMMAND ----------
# MAGIC # Import wheel utilities
# MAGIC from stock_market_utils import calculate_rolling_volatility
# MAGIC from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, count, stddev, round as spark_round, current_timestamp, when
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Read from Silver layer
# MAGIC silver_table = "databricks_course.user_schema.silver_stock_market_returns"
# MAGIC df_gold = spark.table(silver_table)
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Calculate 30-day rolling volatility using wheel utility
# MAGIC df_gold_detailed = calculate_rolling_volatility(df_gold, window_days=30)
# MAGIC
# MAGIC # Save detailed analytics
# MAGIC gold_detailed_table = "databricks_course.user_schema.gold_stock_market_detailed_analytics"
# MAGIC df_gold_detailed.write.format("delta").mode("overwrite").saveAsTable(gold_detailed_table)
# MAGIC
# MAGIC print(f"✅ Gold detailed layer saved: {gold_detailed_table}")
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Create aggregate summary by symbol
# MAGIC df_summary = df_gold.groupBy("symbol").agg(
# MAGIC     spark_min("date").alias("first_date"),
# MAGIC     spark_max("date").alias("last_date"),
# MAGIC     count("*").alias("trading_days"),
# MAGIC     spark_round(spark_max("cumulative_return"), 2).alias("total_return_pct"),
# MAGIC     spark_round(avg("daily_return"), 4).alias("avg_daily_return"),
# MAGIC     spark_round(stddev("daily_return"), 4).alias("volatility"),
# MAGIC     spark_round(avg("volume"), 0).alias("avg_daily_volume")
# MAGIC )
# MAGIC
# MAGIC # Add performance classification
# MAGIC df_summary = df_summary.withColumn(
# MAGIC     "performance_tier",
# MAGIC     when(col("total_return_pct") > 50, "🔥 High Performer")
# MAGIC     .when(col("total_return_pct") > 20, "⭐ Good Performer")
# MAGIC     .when(col("total_return_pct") > 0, "✅ Positive")
# MAGIC     .otherwise("❌ Negative")
# MAGIC )
# MAGIC
# MAGIC df_summary = df_summary.withColumn("updated_at", current_timestamp())
# MAGIC
# MAGIC # COMMAND ----------
# MAGIC # Save summary gold table
# MAGIC gold_summary_table = "databricks_course.user_schema.gold_stock_market_summary"
# MAGIC df_summary.write.format("delta").mode("overwrite").saveAsTable(gold_summary_table)
# MAGIC
# MAGIC print(f"✅ Gold summary layer saved: {gold_summary_table}")
# MAGIC print("\n📊 Stock Market Performance Summary:")
# MAGIC display(df_summary.orderBy(col("total_return_pct").desc()))
# MAGIC ```
# MAGIC
# MAGIC **Gold Layer Output**:
# MAGIC - Detailed analytics with 30-day rolling metrics
# MAGIC - Summary table with performance tiers for business stakeholders
# MAGIC - Ready for consumption by BI tools or Databricks Apps
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Complete Pipeline Summary
# MAGIC
# MAGIC | Layer | Notebook | Purpose | Wheel Functions Used |
# MAGIC |-------|----------|---------|---------------------|
# MAGIC | **Bronze** | `bronze_ingestion.py` | Ingest raw data from Yahoo Finance | `ingest_stock_data_to_spark()`, `save_to_bronze()`, `validate_stock_data()` |
# MAGIC | **Silver** | `silver_transformations.py` | Calculate returns and metrics | `calculate_daily_returns()`, `calculate_cumulative_returns()`, `calculate_price_range_metrics()` |
# MAGIC | **Gold** | `gold_analytics.py` | Create business-ready analytics | `calculate_rolling_volatility()` |
# MAGIC
# MAGIC **Production Deployment**:
# MAGIC 1. Build wheel: `poetry build` → `stock_market_utils-1.0.0.whl`
# MAGIC 2. Upload to Unity Catalog Volume
# MAGIC 3. Create job with 3 tasks (Bronze → Silver → Gold)
# MAGIC 4. Attach wheel to all tasks
# MAGIC 5. Schedule and monitor

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Wheel Deployment to Databricks
# MAGIC
# MAGIC This section explains **how to deploy your Python wheel to Databricks** for production use.
# MAGIC
# MAGIC > **Note**: This is a conceptual guide. For actual deployment, follow these steps in your workspace.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1: Create Unity Catalog Volume
# MAGIC
# MAGIC Unity Catalog Volumes provide **governed storage** for files including Python wheels, JARs, and other artifacts.
# MAGIC
# MAGIC **Using SQL**:
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS databricks_course.{your_schema}.production_libraries
# MAGIC COMMENT 'Production Python wheels and artifacts';
# MAGIC ```
# MAGIC
# MAGIC **Using Python/PySpark**:
# MAGIC ```python
# MAGIC # Create volume for production libraries
# MAGIC spark.sql("""
# MAGIC CREATE VOLUME IF NOT EXISTS databricks_course.{your_schema}.production_libraries
# MAGIC COMMENT 'Production Python wheels for data pipelines'
# MAGIC """)
# MAGIC
# MAGIC # Verify creation
# MAGIC spark.sql("SHOW VOLUMES IN databricks_course.{your_schema}").display()
# MAGIC ```
# MAGIC
# MAGIC **Result**: Volume created at path `/Volumes/databricks_course/{your_schema}/production_libraries/`
# MAGIC
# MAGIC **Benefits of Unity Catalog Volumes**:
# MAGIC - ✅ **Governed**: Access controlled via Unity Catalog permissions
# MAGIC - ✅ **Versioned**: Can store multiple wheel versions (`v1.0.0`, `v1.1.0`, etc.)
# MAGIC - ✅ **Shareable**: Team members can access shared libraries
# MAGIC - ✅ **Auditable**: All access logged in Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2: Upload Wheel to Volume
# MAGIC
# MAGIC After building your wheel locally (`poetry build`), upload it to the Unity Catalog Volume.
# MAGIC
# MAGIC ### Option 1: Databricks CLI (Recommended for Automation)
# MAGIC
# MAGIC ```bash
# MAGIC # Build the wheel locally
# MAGIC cd stock-market-utils
# MAGIC poetry build
# MAGIC
# MAGIC # Upload to Unity Catalog Volume
# MAGIC databricks fs cp \
# MAGIC   dist/stock_market_utils-1.0.0-py3-none-any.whl \
# MAGIC   /Volumes/databricks_course/{your_schema}/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl
# MAGIC
# MAGIC # Verify upload
# MAGIC databricks fs ls /Volumes/databricks_course/{your_schema}/production_libraries/
# MAGIC
# MAGIC # Expected output:
# MAGIC # stock_market_utils-1.0.0-py3-none-any.whl
# MAGIC ```
# MAGIC
# MAGIC ### Option 2: Databricks UI (Good for One-Time Uploads)
# MAGIC
# MAGIC 1. Navigate to **Catalog** in left sidebar
# MAGIC 2. Browse to your schema: `databricks_course` → `{your_schema}` → `production_libraries`
# MAGIC 3. Click **Upload** button (top right)
# MAGIC 4. Select `stock_market_utils-1.0.0-py3-none-any.whl` from your local `dist/` folder
# MAGIC 5. Click **Upload**
# MAGIC
# MAGIC **Result**: Wheel uploaded and ready to use!
# MAGIC
# MAGIC ### Option 3: Python (For Programmatic Deployment)
# MAGIC
# MAGIC ```python
# MAGIC # Using dbutils (in a Databricks notebook)
# MAGIC dbutils.fs.cp(
# MAGIC     "file:/Workspace/Users/{your_email}/stock-market-utils/dist/stock_market_utils-1.0.0-py3-none-any.whl",
# MAGIC     "/Volumes/databricks_course/{your_schema}/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl"
# MAGIC )
# MAGIC
# MAGIC # Verify
# MAGIC display(dbutils.fs.ls("/Volumes/databricks_course/{your_schema}/production_libraries/"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3: Version Management
# MAGIC
# MAGIC For production systems, maintain multiple versions:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/databricks_course/{schema}/production_libraries/
# MAGIC ├── stock_market_utils-1.0.0-py3-none-any.whl  (stable)
# MAGIC ├── stock_market_utils-1.1.0-py3-none-any.whl  (new features)
# MAGIC └── stock_market_utils-1.1.1-py3-none-any.whl  (bug fixes)
# MAGIC ```
# MAGIC
# MAGIC **Best Practices**:
# MAGIC - Use semantic versioning (MAJOR.MINOR.PATCH)
# MAGIC - Test new versions in dev/staging before production
# MAGIC - Document breaking changes in release notes
# MAGIC - Keep at least 2-3 recent versions for rollback
# MAGIC
# MAGIC **Updating Jobs to New Version**:
# MAGIC ```python
# MAGIC # Update job library configuration
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Get job
# MAGIC job = w.jobs.get(job_id)
# MAGIC
# MAGIC # Update wheel path to new version
# MAGIC new_libraries = [
# MAGIC     jobs.Library(whl="/Volumes/databricks_course/{schema}/production_libraries/stock_market_utils-1.1.0-py3-none-any.whl")
# MAGIC ]
# MAGIC
# MAGIC # Update job
# MAGIC w.jobs.update(job_id=job_id, new_settings=jobs.JobSettings(tasks=[...]))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4: Using the Wheel in Notebooks
# MAGIC
# MAGIC Once uploaded, use the wheel in any notebook or job:
# MAGIC
# MAGIC **Install in Interactive Notebook**:
# MAGIC ```python
# MAGIC %pip install /Volumes/databricks_course/{your_schema}/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl
# MAGIC
# MAGIC # Restart Python to load the library
# MAGIC dbutils.library.restartPython()
# MAGIC
# MAGIC # Now you can import and use
# MAGIC from stock_market_utils import ingest_stock_data_to_spark, calculate_daily_returns
# MAGIC
# MAGIC df = ingest_stock_data_to_spark(spark, ["AAPL", "GOOGL"], "2024-01-01", "2024-12-31")
# MAGIC df_with_returns = calculate_daily_returns(df)
# MAGIC ```
# MAGIC
# MAGIC **Attach to Job Cluster** (Preferred for Production):
# MAGIC - Navigate to Jobs → Select your job → Edit
# MAGIC - Go to **Libraries** tab
# MAGIC - Click **Add** → **Workspace**
# MAGIC - Browse to `/Volumes/databricks_course/{schema}/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl`
# MAGIC - Click **Add**
# MAGIC
# MAGIC **Benefits of Cluster-Level Installation**:
# MAGIC - No need for `%pip install` in every notebook
# MAGIC - Wheel available to all tasks in the job
# MAGIC - Faster execution (pre-installed on cluster startup)
# MAGIC - Consistent versioning across all tasks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Grant Access to Team (Optional)
# MAGIC
# MAGIC Allow other team members to use your wheel:
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant read access to volume
# MAGIC GRANT READ VOLUME ON VOLUME databricks_course.{your_schema}.production_libraries
# MAGIC TO `data-engineering-team@company.com`;
# MAGIC
# MAGIC -- Grant write access for updates
# MAGIC GRANT WRITE VOLUME ON VOLUME databricks_course.{your_schema}.production_libraries
# MAGIC TO `platform-admins@company.com`;
# MAGIC ```
# MAGIC
# MAGIC **Team Workflow**:
# MAGIC 1. Platform engineers upload new wheel versions
# MAGIC 2. Data engineers reference wheel in their jobs
# MAGIC 3. All permissions managed centrally in Unity Catalog
# MAGIC 4. Audit log tracks all wheel downloads and usage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Deployment Checklist
# MAGIC
# MAGIC Before deploying to production:
# MAGIC
# MAGIC - [ ] **Local Testing**: Wheel installs and imports correctly
# MAGIC - [ ] **Unit Tests**: All tests passing (`poetry run pytest`)
# MAGIC - [ ] **Version Bump**: Updated version in `pyproject.toml`
# MAGIC - [ ] **Build**: Wheel built successfully (`poetry build`)
# MAGIC - [ ] **Upload**: Wheel uploaded to Unity Catalog Volume
# MAGIC - [ ] **Access**: Team members have appropriate permissions
# MAGIC - [ ] **Documentation**: README and docstrings updated
# MAGIC - [ ] **Job Update**: Production jobs using new wheel version
# MAGIC - [ ] **Monitoring**: Jobs running successfully with new version
# MAGIC - [ ] **Rollback Plan**: Previous version retained for quick rollback

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4: Production Job Orchestration - Conceptual Guide
# MAGIC
# MAGIC This section explains **how you would orchestrate a wheel-based pipeline in production**.
# MAGIC
# MAGIC > **Note**: For hands-on job orchestration practice, see **Notebook 19** which orchestrates actual notebooks (06-09) using both UI and SDK approaches.
# MAGIC
# MAGIC ## Production Deployment Approaches
# MAGIC
# MAGIC When deploying wheel-based pipelines to production, you have **two main options**:
# MAGIC
# MAGIC ### Option 1: Separate Notebooks per Layer (Recommended)
# MAGIC
# MAGIC **Structure**:
# MAGIC ```
# MAGIC stock-market-pipeline/
# MAGIC ├── bronze_ingestion.py      # Imports wheel, runs bronze logic
# MAGIC ├── silver_transformations.py # Imports wheel, runs silver logic
# MAGIC └── gold_analytics.py         # Imports wheel, runs gold logic
# MAGIC ```
# MAGIC
# MAGIC **Job Configuration**:
# MAGIC - **Task 1** → Run `bronze_ingestion.py` with wheel attached
# MAGIC - **Task 2** → Run `silver_transformations.py` (depends on Task 1)
# MAGIC - **Task 3** → Run `gold_analytics.py` (depends on Task 2)
# MAGIC
# MAGIC **Advantages**:
# MAGIC - ✅ Clean separation of concerns
# MAGIC - ✅ Easy to test individual layers
# MAGIC - ✅ Clear dependencies in job DAG
# MAGIC - ✅ Can run layers independently
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Option 2: Python Files/Scripts (Advanced)
# MAGIC
# MAGIC **Structure**:
# MAGIC ```python
# MAGIC # bronze_ingestion.py (Python file, not notebook)
# MAGIC from stock_market_utils import ingest_stock_data_to_spark, save_to_bronze
# MAGIC from databricks.sdk.runtime import spark
# MAGIC
# MAGIC # Get parameters
# MAGIC symbols = dbutils.widgets.get("symbols").split(",")
# MAGIC start_date = dbutils.widgets.get("start_date")
# MAGIC end_date = dbutils.widgets.get("end_date")
# MAGIC
# MAGIC # Ingest data using wheel utilities
# MAGIC df = ingest_stock_data_to_spark(spark, symbols, start_date, end_date)
# MAGIC save_to_bronze(df, "bronze_stock_market_raw")
# MAGIC ```
# MAGIC
# MAGIC **Job Configuration**:
# MAGIC - Use **Python task** type (not Notebook task)
# MAGIC - Attach wheel as library dependency
# MAGIC - Pass parameters via job configuration
# MAGIC
# MAGIC **Advantages**:
# MAGIC - ✅ True modular code (not Databricks-specific)
# MAGIC - ✅ Easier to unit test locally
# MAGIC - ✅ Better version control integration
# MAGIC - ✅ Professional production pattern
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Example Job Configuration (UI Approach)
# MAGIC
# MAGIC Here's how you would configure a multi-task job in the Databricks UI:
# MAGIC
# MAGIC ### Job Setup
# MAGIC ```
# MAGIC Name: Stock Market Pipeline - Production
# MAGIC Description: Medallion architecture pipeline using stock_market_utils wheel
# MAGIC Schedule: 0 18 * * 1-5 (6 PM weekdays, after market close)
# MAGIC Max concurrent runs: 1
# MAGIC ```
# MAGIC
# MAGIC ### Task 1: Bronze Layer
# MAGIC ```
# MAGIC Task name: ingest_stock_data
# MAGIC Type: Notebook (or Python file)
# MAGIC Path: /Workspace/pipelines/bronze_ingestion.py
# MAGIC Cluster: New job cluster (14.3 LTS, i3.xlarge, 2 workers)
# MAGIC Libraries:
# MAGIC   - Wheel: /Volumes/databricks_course/{schema}/libraries/stock_market_utils-1.0.0.whl
# MAGIC   - PyPI: yfinance
# MAGIC Parameters:
# MAGIC   - symbols: AAPL,GOOGL,MSFT,AMZN,NVDA
# MAGIC   - start_date: 2024-01-01
# MAGIC   - end_date: 2024-12-31
# MAGIC Timeout: 3600 seconds
# MAGIC Retries: 2
# MAGIC ```
# MAGIC
# MAGIC ### Task 2: Silver Layer
# MAGIC ```
# MAGIC Task name: calculate_returns
# MAGIC Type: Notebook (or Python file)
# MAGIC Path: /Workspace/pipelines/silver_transformations.py
# MAGIC Depends on: ingest_stock_data
# MAGIC Cluster: Use same cluster
# MAGIC Libraries: Same as Task 1
# MAGIC Timeout: 1800 seconds
# MAGIC Retries: 2
# MAGIC ```
# MAGIC
# MAGIC ### Task 3: Gold Layer
# MAGIC ```
# MAGIC Task name: aggregate_insights
# MAGIC Type: Notebook (or Python file)
# MAGIC Path: /Workspace/pipelines/gold_analytics.py
# MAGIC Depends on: calculate_returns
# MAGIC Cluster: Use same cluster
# MAGIC Libraries: Same as Task 1
# MAGIC Timeout: 1800 seconds
# MAGIC Retries: 1
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Differences from Notebook 19
# MAGIC
# MAGIC | Aspect | Notebook 19 (Practical) | Notebook 21 (Conceptual) |
# MAGIC |--------|------------------------|--------------------------|
# MAGIC | **Purpose** | Hands-on job creation | Production wheel pattern |
# MAGIC | **Orchestrates** | Existing notebooks (06-09) | Wheel-based notebooks/scripts |
# MAGIC | **Libraries** | No wheels needed | Requires wheel deployment |
# MAGIC | **Complexity** | Beginner-friendly | Production-ready |
# MAGIC | **Use case** | Learning orchestration | Real-world deployment |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## When to Use This Pattern
# MAGIC
# MAGIC Use wheel-based orchestration when:
# MAGIC - ✅ You have complex, reusable transformation logic
# MAGIC - ✅ Multiple pipelines share the same utilities
# MAGIC - ✅ You need version control for business logic
# MAGIC - ✅ You want to unit test transformations
# MAGIC - ✅ You're building production-grade data products
# MAGIC
# MAGIC For simpler pipelines or learning, **Notebook 19's approach** (orchestrating notebooks directly) is perfectly valid!

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 5: Job Orchestration - SDK Approach
# MAGIC
# MAGIC Now let's create the same job programmatically using the Databricks SDK.
# MAGIC
# MAGIC ## Advantages of SDK Approach
# MAGIC - **Version controlled**: Job definition in code
# MAGIC - **Reproducible**: Easy to recreate in different environments
# MAGIC - **Automated**: Part of CI/CD pipelines
# MAGIC - **Parameterized**: Easy to create variations
# MAGIC - **Scalable**: Create multiple jobs programmatically

# COMMAND ----------

# Import Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import Task, NotebookTask, Source

# Initialize client
w = WorkspaceClient()

print("✅ Databricks SDK initialized")

# COMMAND ----------

# Job configuration
JOB_NAME = "Stock Market Pipeline - Production (SDK)"
NOTEBOOK_PATH = "/Workspace/course/notebooks/05_week/21_stock_market_wheel_deployment"
WHEEL_PATH = f"/Volumes/{CATALOG}/{USER_SCHEMA}/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl"

# Stock symbols and date range
SYMBOLS_PARAM = "AAPL,GOOGL,MSFT,AMZN,NVDA"
START_DATE_PARAM = "2024-01-01"
END_DATE_PARAM = "2024-12-31"

print(f"📝 Job Configuration:")
print(f"   Name: {JOB_NAME}")
print(f"   Notebook: {NOTEBOOK_PATH}")
print(f"   Wheel: {WHEEL_PATH}")
print(f"   Symbols: {SYMBOLS_PARAM}")
print(f"   Date Range: {START_DATE_PARAM} to {END_DATE_PARAM}")

# COMMAND ----------

# Define cluster configuration 
cluster_config = jobs.ClusterSpec(
    spark_version="14.3.x-scala2.12",
    node_type_id="i3.xlarge",
    num_workers=2,
    spark_conf={
        "spark.databricks.delta.preview.enabled": "true"
    }
)

# Define shared libraries for all tasks
shared_libraries = [
    jobs.Library(whl=WHEEL_PATH),
    jobs.Library(pypi=jobs.PythonPyPiLibrary(package="yfinance"))
]

print("✅ Cluster configuration defined")

# COMMAND ----------

# Define tasks
tasks = [
    # Task 1: Bronze - Ingest stock data
    Task(
        task_key="bronze_ingest_stock_data",
        description="Ingest raw stock market data from Yahoo Finance API",
        notebook_task=NotebookTask(
            notebook_path=NOTEBOOK_PATH,
            source=Source.WORKSPACE,
            base_parameters={
                "symbols": SYMBOLS_PARAM,
                "start_date": START_DATE_PARAM,
                "end_date": END_DATE_PARAM,
                "layer": "bronze"
            }
        ),
        new_cluster=cluster_config,
        libraries=shared_libraries,
        timeout_seconds=3600,
        max_retries=2,
        min_retry_interval_millis=60000
    ),

    # Task 2: Silver - Calculate returns and metrics
    Task(
        task_key="silver_calculate_returns",
        description="Calculate daily returns, cumulative returns, and price metrics",
        notebook_task=NotebookTask(
            notebook_path=NOTEBOOK_PATH,
            source=Source.WORKSPACE,
            base_parameters={"layer": "silver"}
        ),
        depends_on=[jobs.TaskDependency(task_key="bronze_ingest_stock_data")],
        new_cluster=cluster_config,
        libraries=shared_libraries,
        timeout_seconds=1800,
        max_retries=2,
        min_retry_interval_millis=60000
    ),

    # Task 3: Gold - Create market insights
    Task(
        task_key="gold_market_insights",
        description="Aggregate analytics and create market performance summary",
        notebook_task=NotebookTask(
            notebook_path=NOTEBOOK_PATH,
            source=Source.WORKSPACE,
            base_parameters={"layer": "gold"}
        ),
        depends_on=[jobs.TaskDependency(task_key="silver_calculate_returns")],
        new_cluster=cluster_config,
        libraries=shared_libraries,
        timeout_seconds=1800,
        max_retries=1,
        min_retry_interval_millis=60000
    )
]

print(f"✅ Defined {len(tasks)} tasks:")
for task in tasks:
    print(f"   - {task.task_key}: {task.description}")

# COMMAND ----------

# Check if job already exists
existing_jobs = list(w.jobs.list(name=JOB_NAME))

if existing_jobs:
    print(f"⚠️  Job '{JOB_NAME}' already exists")
    job_id = existing_jobs[0].job_id
    print(f"   Job ID: {job_id}")
    print(f"   Updating existing job...")

    # Update existing job
    w.jobs.update(
        job_id=job_id,
        new_settings=jobs.JobSettings(
            name=JOB_NAME,
            tasks=tasks,
            email_notifications=jobs.JobEmailNotifications(
                on_success=[w.current_user.me().user_name],
                on_failure=[w.current_user.me().user_name]
            ),
            max_concurrent_runs=1,
            timeout_seconds=7200
        )
    )
    print(f"✅ Job updated successfully")
else:
    print(f"📝 Creating new job: {JOB_NAME}")

    # Create new job
    created_job = w.jobs.create(
        name=JOB_NAME,
        tasks=tasks,
        email_notifications=jobs.JobEmailNotifications(
            on_success=[w.current_user.me().user_name],
            on_failure=[w.current_user.me().user_name]
        ),
        max_concurrent_runs=1,
        timeout_seconds=7200
    )

    job_id = created_job.job_id
    print(f"✅ Job created successfully")
    print(f"   Job ID: {job_id}")

# Get job URL
job_url = f"{w.config.host}#job/{job_id}"
print(f"\n🔗 Job URL: {job_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Job and Monitor Progress

# COMMAND ----------

# Trigger job run
print(f"🚀 Triggering job run for: {JOB_NAME}")
run = w.jobs.run_now(job_id=job_id)
run_id = run.run_id

print(f"✅ Job run started")
print(f"   Run ID: {run_id}")
print(f"   Job ID: {job_id}")
print(f"🔗 Run URL: {w.config.host}#job/{job_id}/run/{run_id}")

# COMMAND ----------

# Monitor job execution in real-time
import time
from datetime import datetime

print(f"\n⏳ Monitoring job execution...\n")
print("=" * 80)

max_wait_seconds = 3600  # 1 hour timeout
check_interval = 10  # Check every 10 seconds
elapsed = 0

while elapsed < max_wait_seconds:
    run_status = w.jobs.get_run(run_id=run_id)
    lifecycle_state = run_status.state.life_cycle_state

    # Print overall status
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Overall Status: {lifecycle_state}")

    # Print individual task status
    if run_status.tasks:
        for task_run in run_status.tasks:
            task_state = task_run.state.life_cycle_state
            task_result = task_run.state.result_state if task_run.state.result_state else "PENDING"

            # Emoji indicators
            if task_result == "SUCCESS":
                emoji = "✅"
            elif task_result == "FAILED":
                emoji = "❌"
            elif task_state == "RUNNING":
                emoji = "🔄"
            else:
                emoji = "⏸️"

            print(f"    {emoji} {task_run.task_key:<30} | {task_state:<12} | {task_result}")

    # Check if run completed
    if lifecycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        print("\n" + "=" * 80)

        final_result = run_status.state.result_state
        if final_result == "SUCCESS":
            print("✅ JOB COMPLETED SUCCESSFULLY")
            print("\n📊 Pipeline Results:")
            print(f"   ✅ Bronze: Stock market data ingested")
            print(f"   ✅ Silver: Returns and metrics calculated")
            print(f"   ✅ Gold: Market insights aggregated")
        else:
            print(f"❌ JOB FAILED: {final_result}")
            if run_status.state.state_message:
                print(f"   Error: {run_status.state.state_message}")

        print("=" * 80)
        break

    time.sleep(check_interval)
    elapsed += check_interval
    print()  # Blank line between status updates

if elapsed >= max_wait_seconds:
    print("⚠️  Monitoring timeout reached. Job may still be running.")
    print(f"   Check status at: {w.config.host}#job/{job_id}/run/{run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Pipeline Results

# COMMAND ----------

# Query the Gold summary table to see results
gold_summary_table = get_table_path("gold", "stock_market_summary")

print(f"📊 Querying results from: {gold_summary_table}\n")

df_results = spark.table(gold_summary_table)

print(f"Total Stocks Analyzed: {df_results.count()}")
print("\n🏆 Top Performers:")
display(df_results.orderBy(col("total_return_pct").desc()))

# COMMAND ----------

# Show detailed analytics for best performer
best_stock = df_results.orderBy(col("total_return_pct").desc()).first()

print(f"🏅 Best Performing Stock: {best_stock['symbol']}")
print(f"   Total Return: {best_stock['total_return_pct']:.2f}%")
print(f"   Avg Daily Return: {best_stock['avg_daily_return']:.4f}%")
print(f"   Volatility: {best_stock['volatility']:.4f}")
print(f"   Performance Tier: {best_stock['performance_tier']}")

# Show price history
gold_detailed_table = get_table_path("gold", "stock_market_detailed_analytics")
df_history = spark.table(gold_detailed_table).filter(col("symbol") == best_stock['symbol'])

print(f"\n📈 Price History for {best_stock['symbol']}:")
display(
    df_history
    .select("date", "close", "daily_return", "cumulative_return", "volatility_30d")
    .orderBy("date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Production Deployment Guide
# MAGIC
# MAGIC ## What We Accomplished
# MAGIC
# MAGIC ✅ **Built a production-ready Python wheel** (`stock_market_utils-1.0.0-py3-none-any.whl`):
# MAGIC - Yahoo Finance integration for data ingestion
# MAGIC - Financial calculations (returns, volatility, metrics)
# MAGIC - Data quality validators
# MAGIC - Comprehensive test suite
# MAGIC
# MAGIC ✅ **Implemented medallion architecture pipeline**:
# MAGIC - **Bronze**: Raw stock market data from Yahoo Finance API
# MAGIC - **Silver**: Calculated returns, metrics, and cleansed data
# MAGIC - **Gold**: Aggregated market insights and performance analytics
# MAGIC
# MAGIC ✅ **Deployed wheel to Unity Catalog Volumes**:
# MAGIC - Governed storage with access control
# MAGIC - Version management for production libraries
# MAGIC - Reusable across all jobs and notebooks
# MAGIC
# MAGIC ✅ **Created production jobs (UI + SDK)**:
# MAGIC - Multi-task orchestration with dependencies
# MAGIC - Automated scheduling and monitoring
# MAGIC - Email notifications and retry logic
# MAGIC - Both approaches demonstrated for flexibility
# MAGIC
# MAGIC ## Production Deployment Checklist
# MAGIC
# MAGIC ### 1. Wheel Development ✅
# MAGIC - [ ] Code written with proper structure (src/ layout)
# MAGIC - [ ] Unit tests created and passing
# MAGIC - [ ] Documentation (docstrings, README)
# MAGIC - [ ] Version managed with semantic versioning
# MAGIC - [ ] Built with Poetry: `poetry build`
# MAGIC
# MAGIC ### 2. Wheel Deployment ✅
# MAGIC - [ ] Unity Catalog Volume created
# MAGIC - [ ] Wheel uploaded to volume
# MAGIC - [ ] Permissions configured for team access
# MAGIC - [ ] Version documented in team wiki
# MAGIC
# MAGIC ### 3. Pipeline Development ✅
# MAGIC - [ ] Bronze layer: Data ingestion and validation
# MAGIC - [ ] Silver layer: Transformations and cleansing
# MAGIC - [ ] Gold layer: Analytics and aggregations
# MAGIC - [ ] Data quality checks at each layer
# MAGIC - [ ] Error handling and logging
# MAGIC
# MAGIC ### 4. Job Orchestration ✅
# MAGIC - [ ] Tasks defined with clear dependencies
# MAGIC - [ ] Cluster sizing appropriate for workload
# MAGIC - [ ] Libraries configured (wheel + dependencies)
# MAGIC - [ ] Parameters externalized for flexibility
# MAGIC - [ ] Retry logic configured
# MAGIC - [ ] Timeout settings reasonable
# MAGIC
# MAGIC ### 5. Monitoring & Alerts ✅
# MAGIC - [ ] Email notifications configured
# MAGIC - [ ] Logging comprehensive
# MAGIC - [ ] Data quality metrics tracked
# MAGIC - [ ] Job run history monitored
# MAGIC - [ ] Alerting for failures configured
# MAGIC
# MAGIC ### 6. Documentation 📝
# MAGIC - [ ] Wheel usage examples
# MAGIC - [ ] Pipeline architecture documented
# MAGIC - [ ] Job scheduling documented
# MAGIC - [ ] Troubleshooting guide created
# MAGIC - [ ] Team runbook prepared
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Wheels enable professional deployment**: Reusable, testable, version-controlled code
# MAGIC 2. **Unity Catalog Volumes are essential**: Modern, governed storage for production libraries
# MAGIC 3. **Both UI and SDK have value**:
# MAGIC    - UI: Great for learning and quick iterations
# MAGIC    - SDK: Perfect for automation and CI/CD
# MAGIC 4. **Medallion architecture scales**: Bronze → Silver → Gold pattern works for any domain
# MAGIC 5. **Real-world data teaches best**: Stock market data demonstrates production patterns
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC After mastering production pipelines, explore **Databricks Apps** (Advanced Section):
# MAGIC - Build interactive dashboards with Streamlit
# MAGIC - Visualize gold layer data for stakeholders
# MAGIC - Create self-service analytics tools
# MAGIC - Deploy data apps for business users
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** 🎉 You've completed the production deployment workflow from development to orchestration!