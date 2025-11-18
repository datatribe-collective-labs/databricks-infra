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
# MAGIC # Part 5: Job Orchestration with SDK - Reference Example
# MAGIC
# MAGIC This section shows **how to automate job creation using the Databricks SDK** for production deployments.
# MAGIC
# MAGIC > **Note**: This is an educational reference. For hands-on SDK practice, see **Notebook 19** which orchestrates real notebooks.
# MAGIC
# MAGIC ## Why Use SDK for Job Creation?
# MAGIC
# MAGIC - ✅ **Version Control**: Job definitions live in Git alongside code
# MAGIC - ✅ **Automation**: Create/update jobs in CI/CD pipelines
# MAGIC - ✅ **Reproducibility**: Deploy identical jobs across dev/staging/prod
# MAGIC - ✅ **Parameterization**: Easily create job variations
# MAGIC - ✅ **Scale**: Manage hundreds of jobs programmatically
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Example: Creating a Multi-Layer Pipeline Job
# MAGIC
# MAGIC Here's how you would create a job that orchestrates Bronze → Silver → Gold transformations using the SDK:
# MAGIC
# MAGIC ### Step 1: Setup and Configuration
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import jobs
# MAGIC from databricks.sdk.service.jobs import Task, NotebookTask, Source, TaskDependency
# MAGIC
# MAGIC # Initialize SDK client (auto-authenticated in Databricks notebooks)
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC print("✅ Databricks SDK initialized")
# MAGIC print(f"   Workspace: {w.config.host}")
# MAGIC print(f"   User: {w.current_user.me().user_name}")
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Define Job Configuration
# MAGIC
# MAGIC ```python
# MAGIC # Job metadata
# MAGIC JOB_NAME = "Stock Market Pipeline - Production"
# MAGIC
# MAGIC # Notebook paths (these would be separate notebooks in production)
# MAGIC BRONZE_NOTEBOOK = "/Workspace/pipelines/bronze_stock_ingestion"
# MAGIC SILVER_NOTEBOOK = "/Workspace/pipelines/silver_stock_transformations"
# MAGIC GOLD_NOTEBOOK = "/Workspace/pipelines/gold_stock_analytics"
# MAGIC
# MAGIC # Wheel path in Unity Catalog Volume
# MAGIC WHEEL_PATH = "/Volumes/databricks_course/user_schema/libraries/stock_market_utils-1.0.0-py3-none-any.whl"
# MAGIC
# MAGIC # Job parameters
# MAGIC SYMBOLS = "AAPL,GOOGL,MSFT,AMZN,NVDA"
# MAGIC START_DATE = "2024-01-01"
# MAGIC END_DATE = "2024-12-31"
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Define Tasks with Dependencies
# MAGIC
# MAGIC ```python
# MAGIC tasks = [
# MAGIC     # Task 1: Bronze Layer (no dependencies - runs first)
# MAGIC     Task(
# MAGIC         task_key="bronze_ingest_stock_data",
# MAGIC         description="Ingest raw stock market data from Yahoo Finance",
# MAGIC         notebook_task=NotebookTask(
# MAGIC             notebook_path=BRONZE_NOTEBOOK,
# MAGIC             source=Source.WORKSPACE,
# MAGIC             base_parameters={
# MAGIC                 "symbols": SYMBOLS,
# MAGIC                 "start_date": START_DATE,
# MAGIC                 "end_date": END_DATE
# MAGIC             }
# MAGIC         ),
# MAGIC         libraries=[
# MAGIC             jobs.Library(whl=WHEEL_PATH),
# MAGIC             jobs.Library(pypi=jobs.PythonPyPiLibrary(package="yfinance"))
# MAGIC         ],
# MAGIC         timeout_seconds=3600,  # 1 hour
# MAGIC         max_retries=2,
# MAGIC         min_retry_interval_millis=60000  # 1 minute between retries
# MAGIC         # NOTE: No cluster config = uses Serverless compute!
# MAGIC     ),
# MAGIC
# MAGIC     # Task 2: Silver Layer (depends on Bronze)
# MAGIC     Task(
# MAGIC         task_key="silver_calculate_returns",
# MAGIC         description="Calculate daily returns and price metrics",
# MAGIC         notebook_task=NotebookTask(
# MAGIC             notebook_path=SILVER_NOTEBOOK,
# MAGIC             source=Source.WORKSPACE
# MAGIC         ),
# MAGIC         depends_on=[TaskDependency(task_key="bronze_ingest_stock_data")],  # Sequential dependency
# MAGIC         libraries=[jobs.Library(whl=WHEEL_PATH)],
# MAGIC         timeout_seconds=1800,  # 30 minutes
# MAGIC         max_retries=2,
# MAGIC         min_retry_interval_millis=60000
# MAGIC     ),
# MAGIC
# MAGIC     # Task 3: Gold Layer (depends on Silver)
# MAGIC     Task(
# MAGIC         task_key="gold_market_insights",
# MAGIC         description="Create market performance analytics",
# MAGIC         notebook_task=NotebookTask(
# MAGIC             notebook_path=GOLD_NOTEBOOK,
# MAGIC             source=Source.WORKSPACE
# MAGIC         ),
# MAGIC         depends_on=[TaskDependency(task_key="silver_calculate_returns")],  # Sequential dependency
# MAGIC         libraries=[jobs.Library(whl=WHEEL_PATH)],
# MAGIC         timeout_seconds=1800,
# MAGIC         max_retries=1,
# MAGIC         min_retry_interval_millis=60000
# MAGIC     )
# MAGIC ]
# MAGIC
# MAGIC print(f"✅ Defined {len(tasks)} tasks with dependencies:")
# MAGIC print(f"   1. bronze_ingest_stock_data (no dependencies)")
# MAGIC print(f"   2. silver_calculate_returns (depends on bronze)")
# MAGIC print(f"   3. gold_market_insights (depends on silver)")
# MAGIC ```
# MAGIC
# MAGIC **Key Points**:
# MAGIC - **No `new_cluster` specified** = Uses **Serverless compute** (instant start, auto-scaling)
# MAGIC - **`depends_on`** creates sequential execution: Bronze → Silver → Gold
# MAGIC - **`libraries`** attaches wheel to each task
# MAGIC - **Retry logic** handles transient failures
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Task Dependency Patterns
# MAGIC
# MAGIC ### Pattern 1: Sequential (Like This Example)
# MAGIC ```
# MAGIC Bronze → Silver → Gold
# MAGIC ```
# MAGIC ```python
# MAGIC # Silver depends on Bronze
# MAGIC depends_on=[TaskDependency(task_key="bronze_ingest_stock_data")]
# MAGIC
# MAGIC # Gold depends on Silver
# MAGIC depends_on=[TaskDependency(task_key="silver_calculate_returns")]
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: Parallel (Like Notebook 19)
# MAGIC ```
# MAGIC ┌─ Ingest Files ──┐
# MAGIC ├─ Ingest API ────┤
# MAGIC ├─ Ingest DB ─────┤  (All run in parallel)
# MAGIC └─ Ingest S3 ─────┘
# MAGIC ```
# MAGIC ```python
# MAGIC # No depends_on = all tasks run in parallel
# MAGIC Task(task_key="ingest_files", ...),  # No dependencies
# MAGIC Task(task_key="ingest_api", ...),    # No dependencies
# MAGIC Task(task_key="ingest_db", ...),     # No dependencies
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: Fan-Out (Multiple layers depend on one)
# MAGIC ```
# MAGIC           ┌─ Silver Sales ───┐
# MAGIC Bronze ─→ ├─ Silver Marketing ┤
# MAGIC           └─ Silver Support ──┘
# MAGIC ```
# MAGIC ```python
# MAGIC # All three depend on bronze
# MAGIC Task(task_key="silver_sales", depends_on=[TaskDependency(task_key="bronze")]),
# MAGIC Task(task_key="silver_marketing", depends_on=[TaskDependency(task_key="bronze")]),
# MAGIC Task(task_key="silver_support", depends_on=[TaskDependency(task_key="bronze")])
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 4: Fan-In (One layer depends on multiple)
# MAGIC ```
# MAGIC Silver Sales ─────┐
# MAGIC Silver Marketing ─┤─→ Gold Dashboard
# MAGIC Silver Support ───┘
# MAGIC ```
# MAGIC ```python
# MAGIC # Gold depends on all three silver tasks
# MAGIC Task(
# MAGIC     task_key="gold_dashboard",
# MAGIC     depends_on=[
# MAGIC         TaskDependency(task_key="silver_sales"),
# MAGIC         TaskDependency(task_key="silver_marketing"),
# MAGIC         TaskDependency(task_key="silver_support")
# MAGIC     ]
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Create or Update the Job
# MAGIC
# MAGIC ```python
# MAGIC # Check if job already exists
# MAGIC existing_jobs = list(w.jobs.list(name=JOB_NAME))
# MAGIC
# MAGIC if existing_jobs:
# MAGIC     print(f"⚠️  Job '{JOB_NAME}' already exists")
# MAGIC     job_id = existing_jobs[0].job_id
# MAGIC     print(f"   Updating existing job (ID: {job_id})")
# MAGIC
# MAGIC     # Update existing job with new configuration
# MAGIC     w.jobs.update(
# MAGIC         job_id=job_id,
# MAGIC         new_settings=jobs.JobSettings(
# MAGIC             name=JOB_NAME,
# MAGIC             tasks=tasks,
# MAGIC             email_notifications=jobs.JobEmailNotifications(
# MAGIC                 on_success=[w.current_user.me().user_name],
# MAGIC                 on_failure=[w.current_user.me().user_name]
# MAGIC             ),
# MAGIC             max_concurrent_runs=1,
# MAGIC             timeout_seconds=7200  # 2 hours
# MAGIC         )
# MAGIC     )
# MAGIC     print("✅ Job updated successfully")
# MAGIC
# MAGIC else:
# MAGIC     print(f"📝 Creating new job: {JOB_NAME}")
# MAGIC
# MAGIC     # Create new job
# MAGIC     created_job = w.jobs.create(
# MAGIC         name=JOB_NAME,
# MAGIC         tasks=tasks,
# MAGIC         email_notifications=jobs.JobEmailNotifications(
# MAGIC             on_success=[w.current_user.me().user_name],
# MAGIC             on_failure=[w.current_user.me().user_name]
# MAGIC         ),
# MAGIC         max_concurrent_runs=1,
# MAGIC         timeout_seconds=7200
# MAGIC     )
# MAGIC
# MAGIC     job_id = created_job.job_id
# MAGIC     print(f"✅ Job created successfully (ID: {job_id})")
# MAGIC
# MAGIC # Get job URL for easy access
# MAGIC job_url = f"{w.config.host}/#job/{job_id}"
# MAGIC print(f"\n🔗 Job URL: {job_url}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Run and Monitor the Job
# MAGIC
# MAGIC ### Trigger Job Run
# MAGIC
# MAGIC ```python
# MAGIC # Trigger job run
# MAGIC print(f"🚀 Triggering job run for: {JOB_NAME}")
# MAGIC run = w.jobs.run_now(job_id=job_id)
# MAGIC run_id = run.run_id
# MAGIC
# MAGIC print(f"✅ Job run started")
# MAGIC print(f"   Run ID: {run_id}")
# MAGIC print(f"   Job ID: {job_id}")
# MAGIC print(f"🔗 Run URL: {w.config.host}/#job/{job_id}/run/{run_id}")
# MAGIC ```
# MAGIC
# MAGIC ### Monitor Execution in Real-Time
# MAGIC
# MAGIC ```python
# MAGIC import time
# MAGIC from datetime import datetime
# MAGIC
# MAGIC print("\n⏳ Monitoring job execution...\n")
# MAGIC print("=" * 80)
# MAGIC
# MAGIC max_wait_seconds = 3600  # 1 hour timeout
# MAGIC check_interval = 10  # Check every 10 seconds
# MAGIC elapsed = 0
# MAGIC
# MAGIC while elapsed < max_wait_seconds:
# MAGIC     run_status = w.jobs.get_run(run_id=run_id)
# MAGIC     lifecycle_state = run_status.state.life_cycle_state
# MAGIC
# MAGIC     # Print overall status
# MAGIC     print(f"[{datetime.now().strftime('%H:%M:%S')}] Overall: {lifecycle_state}")
# MAGIC
# MAGIC     # Print task-level status
# MAGIC     if run_status.tasks:
# MAGIC         for task_run in run_status.tasks:
# MAGIC             task_state = task_run.state.life_cycle_state
# MAGIC             task_result = task_run.state.result_state or "PENDING"
# MAGIC
# MAGIC             # Status indicators
# MAGIC             emoji = "✅" if task_result == "SUCCESS" else \
# MAGIC                     "❌" if task_result == "FAILED" else \
# MAGIC                     "🔄" if task_state == "RUNNING" else "⏸️"
# MAGIC
# MAGIC             print(f"    {emoji} {task_run.task_key:<30} | {task_state:<12} | {task_result}")
# MAGIC
# MAGIC     # Check if completed
# MAGIC     if lifecycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
# MAGIC         print("\n" + "=" * 80)
# MAGIC
# MAGIC         final_result = run_status.state.result_state
# MAGIC         if final_result == "SUCCESS":
# MAGIC             print("✅ JOB COMPLETED SUCCESSFULLY")
# MAGIC             print("\n📊 Pipeline Results:")
# MAGIC             print("   ✅ Bronze: Stock market data ingested")
# MAGIC             print("   ✅ Silver: Returns and metrics calculated")
# MAGIC             print("   ✅ Gold: Market insights aggregated")
# MAGIC         else:
# MAGIC             print(f"❌ JOB FAILED: {final_result}")
# MAGIC
# MAGIC         print("=" * 80)
# MAGIC         break
# MAGIC
# MAGIC     time.sleep(check_interval)
# MAGIC     elapsed += check_interval
# MAGIC     print()
# MAGIC
# MAGIC if elapsed >= max_wait_seconds:
# MAGIC     print("⚠️  Monitoring timeout. Job may still be running.")
# MAGIC     print(f"   Check: {w.config.host}/#job/{job_id}/run/{run_id}")
# MAGIC ```
# MAGIC
# MAGIC **Expected Output**:
# MAGIC ```
# MAGIC ⏳ Monitoring job execution...
# MAGIC ================================================================================
# MAGIC [14:30:15] Overall: RUNNING
# MAGIC     🔄 bronze_ingest_stock_data        | RUNNING      | PENDING
# MAGIC     ⏸️  silver_calculate_returns        | PENDING      | PENDING
# MAGIC     ⏸️  gold_market_insights            | PENDING      | PENDING
# MAGIC
# MAGIC [14:30:25] Overall: RUNNING
# MAGIC     ✅ bronze_ingest_stock_data        | TERMINATED   | SUCCESS
# MAGIC     🔄 silver_calculate_returns        | RUNNING      | PENDING
# MAGIC     ⏸️  gold_market_insights            | PENDING      | PENDING
# MAGIC
# MAGIC [14:30:35] Overall: RUNNING
# MAGIC     ✅ bronze_ingest_stock_data        | TERMINATED   | SUCCESS
# MAGIC     ✅ silver_calculate_returns        | TERMINATED   | SUCCESS
# MAGIC     🔄 gold_market_insights            | RUNNING      | PENDING
# MAGIC
# MAGIC [14:30:45] Overall: TERMINATED
# MAGIC     ✅ bronze_ingest_stock_data        | TERMINATED   | SUCCESS
# MAGIC     ✅ silver_calculate_returns        | TERMINATED   | SUCCESS
# MAGIC     ✅ gold_market_insights            | TERMINATED   | SUCCESS
# MAGIC
# MAGIC ================================================================================
# MAGIC ✅ JOB COMPLETED SUCCESSFULLY
# MAGIC
# MAGIC 📊 Pipeline Results:
# MAGIC    ✅ Bronze: Stock market data ingested
# MAGIC    ✅ Silver: Returns and metrics calculated
# MAGIC    ✅ Gold: Market insights aggregated
# MAGIC ================================================================================
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 6: Verify Results
# MAGIC
# MAGIC ### Query Gold Summary Table
# MAGIC
# MAGIC ```python
# MAGIC # Query the Gold summary table
# MAGIC gold_summary_table = "databricks_course.user_schema.gold_stock_market_summary"
# MAGIC
# MAGIC print(f"📊 Querying results from: {gold_summary_table}\n")
# MAGIC df_results = spark.table(gold_summary_table)
# MAGIC
# MAGIC print(f"Total Stocks Analyzed: {df_results.count()}")
# MAGIC print("\n🏆 Top Performers:")
# MAGIC display(df_results.orderBy(col("total_return_pct").desc()))
# MAGIC ```
# MAGIC
# MAGIC **Expected Output**:
# MAGIC ```
# MAGIC Total Stocks Analyzed: 5
# MAGIC
# MAGIC 🏆 Top Performers:
# MAGIC
# MAGIC | symbol | total_return_pct | avg_daily_return | volatility | performance_tier   |
# MAGIC |--------|-----------------|------------------|------------|--------------------|
# MAGIC | NVDA   | 185.23          | 0.7451           | 3.2145     | 🔥 High Performer |
# MAGIC | MSFT   | 42.67           | 0.1823           | 1.8234     | ⭐ Good Performer |
# MAGIC | AAPL   | 38.92           | 0.1654           | 1.6782     | ⭐ Good Performer |
# MAGIC | GOOGL  | 31.45           | 0.1345           | 1.9123     | ⭐ Good Performer |
# MAGIC | AMZN   | 28.73           | 0.1234           | 2.0456     | ⭐ Good Performer |
# MAGIC ```
# MAGIC
# MAGIC ### Analyze Best Performer
# MAGIC
# MAGIC ```python
# MAGIC # Get best performing stock
# MAGIC best_stock = df_results.orderBy(col("total_return_pct").desc()).first()
# MAGIC
# MAGIC print(f"🏅 Best Performing Stock: {best_stock['symbol']}")
# MAGIC print(f"   Total Return: {best_stock['total_return_pct']:.2f}%")
# MAGIC print(f"   Avg Daily Return: {best_stock['avg_daily_return']:.4f}%")
# MAGIC print(f"   Volatility: {best_stock['volatility']:.4f}")
# MAGIC print(f"   Performance Tier: {best_stock['performance_tier']}")
# MAGIC
# MAGIC # Show price history
# MAGIC gold_detailed_table = "databricks_course.user_schema.gold_stock_market_detailed_analytics"
# MAGIC df_history = spark.table(gold_detailed_table).filter(col("symbol") == best_stock['symbol'])
# MAGIC
# MAGIC print(f"\n📈 Price History for {best_stock['symbol']}:")
# MAGIC display(
# MAGIC     df_history
# MAGIC     .select("date", "close", "daily_return", "cumulative_return", "volatility_30d")
# MAGIC     .orderBy("date")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **What This Shows**:
# MAGIC - ✅ Complete pipeline successfully orchestrated Bronze → Silver → Gold
# MAGIC - ✅ Serverless compute handled all tasks efficiently (no cluster config needed)
# MAGIC - ✅ Task dependencies ensured correct execution order
# MAGIC - ✅ Wheel utilities provided consistent business logic across all layers
# MAGIC - ✅ Real-time monitoring showed progress and caught any errors
# MAGIC - ✅ Results queryable immediately after job completion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Comparison: Notebook 19 vs Notebook 21
# MAGIC
# MAGIC | Feature | Notebook 19 (Practical) | Notebook 21 (Educational) |
# MAGIC |---------|------------------------|---------------------------|
# MAGIC | **Executable Code** | ✅ Yes - run it yourself! | ❌ No - reference only |
# MAGIC | **Orchestrates** | Notebooks 06-09 (file, API, DB, S3) | Wheel-based pipeline (Bronze/Silver/Gold) |
# MAGIC | **Dependencies** | Parallel (all tasks run together) | Sequential (Bronze → Silver → Gold) |
# MAGIC | **Libraries** | No wheels needed | Requires wheel deployment |
# MAGIC | **Purpose** | Learn job orchestration basics | See production wheel patterns |
# MAGIC | **Best For** | Hands-on learning | Understanding real-world deployment |
# MAGIC
# MAGIC **Key Insight**: Notebook 19 gives you practical experience, Notebook 21 shows you production patterns!

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