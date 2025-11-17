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
# MAGIC # Part 2: Stock Market Data Pipeline Implementation
# MAGIC
# MAGIC Now let's use the wheel to build our production pipeline in Databricks.
# MAGIC
# MAGIC ## Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC Bronze Layer (Raw Data)
# MAGIC   ├── Ingest from Yahoo Finance API
# MAGIC   ├── Store raw OHLCV data
# MAGIC   └── Validate data quality
# MAGIC       ↓
# MAGIC Silver Layer (Cleaned & Enriched)
# MAGIC   ├── Calculate daily returns
# MAGIC   ├── Calculate cumulative returns
# MAGIC   ├── Add price range metrics
# MAGIC   └── Remove any duplicates
# MAGIC       ↓
# MAGIC Gold Layer (Analytics-Ready)
# MAGIC   ├── Aggregate by symbol
# MAGIC   ├── Calculate volatility metrics
# MAGIC   ├── Compute performance statistics
# MAGIC   └── Create market summary
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../utils/user_schema_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies
# MAGIC
# MAGIC For this demonstration, we'll install yfinance directly in the notebook.
# MAGIC In production, this would be part of the wheel dependencies.

# COMMAND ----------

%pip install yfinance --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import col, round as spark_round, lag, stddev, avg, min as spark_min, max as spark_max, count, when, current_timestamp, sum as spark_sum, round as spark_round, first
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

print("✅ Libraries imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Ingest Raw Stock Data

# COMMAND ----------

# Configuration
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "NVDA"]  # Tech stocks
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"

print(f"📈 Fetching stock data for: {', '.join(SYMBOLS)}")
print(f"📅 Date range: {START_DATE} to {END_DATE}")

# COMMAND ----------

# Define stock data schema
stock_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("date", TimestampType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("dividends", DoubleType(), True),
    StructField("stock_splits", DoubleType(), True),
])

# Fetch data from Yahoo Finance (simulating wheel utility)
all_data = []

for symbol in SYMBOLS:
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=START_DATE, end=END_DATE, interval="1d")

        if hist.empty:
            print(f"⚠️  No data found for {symbol}")
            continue

        hist = hist.reset_index()
        hist["symbol"] = symbol
        hist = hist.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "Dividends": "dividends",
            "Stock Splits": "stock_splits"
        })

        hist = hist[["symbol", "date", "open", "high", "low", "close", "volume", "dividends", "stock_splits"]]
        all_data.append(hist)
        print(f"✅ Fetched {len(hist)} records for {symbol}")

    except Exception as e:
        print(f"❌ Error fetching {symbol}: {str(e)}")

# Combine and convert to Spark DataFrame
pandas_df = pd.concat(all_data, ignore_index=True)
df_bronze = spark.createDataFrame(pandas_df, schema=stock_schema)

print(f"\n📊 Total records fetched: {df_bronze.count():,}")

# COMMAND ----------

# Validate data quality
print("🔍 Running data quality checks...")

total_records = df_bronze.count()
null_symbol = df_bronze.filter(col("symbol").isNull()).count()
null_date = df_bronze.filter(col("date").isNull()).count()
null_close = df_bronze.filter(col("close").isNull()).count()
negative_prices = df_bronze.filter(
    (col("open") < 0) | (col("high") < 0) | (col("low") < 0) | (col("close") < 0)
).count()
invalid_ranges = df_bronze.filter(col("high") < col("low")).count()
duplicates = df_bronze.groupBy("symbol", "date").count().filter(col("count") > 1).count()

print("=" * 60)
print("📊 DATA QUALITY REPORT")
print("=" * 60)
print(f"Total Records: {total_records:,}")
print(f"Null Symbols: {null_symbol}")
print(f"Null Dates: {null_date}")
print(f"Null Prices: {null_close}")
print(f"Negative Prices: {negative_prices}")
print(f"Invalid Ranges: {invalid_ranges}")
print(f"Duplicates: {duplicates}")

is_valid = (null_symbol + null_date + null_close + negative_prices + invalid_ranges + duplicates) == 0
print()
if is_valid:
    print("✅ VALIDATION PASSED - Data is ready for processing")
else:
    print("❌ VALIDATION FAILED - Please review data quality issues")
print("=" * 60)

# COMMAND ----------

# Save to Bronze Delta table
bronze_table = get_table_path("bronze", "stock_market_raw")

df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"✅ Bronze layer saved: {bronze_table}")
print(f"   Records: {df_bronze.count():,}")
print(f"   Symbols: {df_bronze.select('symbol').distinct().count()}")

# Display sample
print("\n📋 Sample records:")
display(df_bronze.orderBy("symbol", "date").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Calculate Returns and Metrics

# COMMAND ----------

# Read from Bronze
df_silver = spark.table(bronze_table)

# Calculate daily returns (simulating wheel utility)
window_spec = Window.partitionBy("symbol").orderBy("date")

df_silver = df_silver.withColumn(
    "previous_close",
    lag("close", 1).over(window_spec)
)

# Calculate daily returns (simulating wheel utility)
df_silver = df_silver.withColumn(
    "daily_return",
    when(col("previous_close").isNotNull() & (col("previous_close") != 0),
        spark_round(
            ((col("close") - col("previous_close")) / col("previous_close")) * 100,
            4
        )
    ).otherwise(None)
)

df_silver = df_silver.drop("previous_close")

print("✅ Calculated daily returns")

# COMMAND ----------

# Calculate cumulative returns (from start date)
# The original implementation with lag and a specific rowsBetween window frame was causing an integer overflow error.
# Using first() over a window is a more robust and clearer way to get the first value in a partition.
# Calculate cumulative returns (from start date)
window_first = Window.partitionBy("symbol").orderBy("date")

df_silver = df_silver.withColumn(
    "first_close",
    first("close").over(window_first)
)

df_silver = df_silver.withColumn(
    "cumulative_return",
    spark_round(
        ((col("close") - col("first_close")) / col("first_close")) * 100,
        4
    )
)

df_silver = df_silver.drop("first_close")

print("✅ Calculated cumulative returns")

# COMMAND ----------

# Calculate price range metrics
df_silver = df_silver.withColumn(
    "daily_range",
    spark_round(col("high") - col("low"), 2)
)

df_silver = df_silver.withColumn(
    "daily_range_pct",
    spark_round(((col("high") - col("low")) / col("close")) * 100, 4)
)

print("✅ Calculated price range metrics")

# COMMAND ----------

# Add processing timestamp
df_silver = df_silver.withColumn("processed_at", current_timestamp())

# Save to Silver Delta table
silver_table = get_table_path("silver", "stock_market_returns")

df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"✅ Silver layer saved: {silver_table}")
print(f"   Records: {df_silver.count():,}")

# Display sample with calculated metrics
print("\n📋 Sample records with returns:")
display(
    df_silver
    .select("symbol", "date", "close", "daily_return", "cumulative_return", "daily_range_pct")
    .orderBy("symbol", "date")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Market Insights and Analytics

# COMMAND ----------

# Read from Silver
df_gold_base = spark.table(silver_table)

# Calculate rolling 30-day volatility
window_30d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-29, 0)

df_gold = df_gold_base.withColumn(
    "volatility_30d",
    spark_round(stddev(col("daily_return")).over(window_30d), 4)
)

df_gold = df_gold.withColumn(
    "avg_return_30d",
    spark_round(avg(col("daily_return")).over(window_30d), 4)
)

print("✅ Calculated 30-day rolling metrics")

# COMMAND ----------

# Save detailed gold table
gold_detailed_table = get_table_path("gold", "stock_market_detailed_analytics")

df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_detailed_table)

print(f"✅ Gold detailed layer saved: {gold_detailed_table}")
display(
    df_gold
    .select("symbol", "date", "close", "daily_return", "volatility_30d", "avg_return_30d")
    .orderBy("symbol", "date")
    .limit(10)
)

# COMMAND ----------

# Create aggregate summary by symbol
df_summary = df_gold_base.groupBy("symbol").agg(
    spark_min("date").alias("first_date"),
    spark_max("date").alias("last_date"),
    count("*").alias("trading_days"),
    spark_round(spark_min("close"), 2).alias("min_close"),
    spark_round(spark_max("close"), 2).alias("max_close"),
    spark_round(avg("close"), 2).alias("avg_close"),
    spark_round(spark_max("cumulative_return"), 2).alias("total_return_pct"),
    spark_round(avg("daily_return"), 4).alias("avg_daily_return"),
    spark_round(stddev("daily_return"), 4).alias("volatility"),
    spark_round(avg("volume"), 0).alias("avg_daily_volume")
)

# Add performance tier
df_summary = df_summary.withColumn(
    "performance_tier",
    when(col("total_return_pct") > 50, "🔥 High Performer")
    .when(col("total_return_pct") > 20, "⭐ Good Performer")
    .when(col("total_return_pct") > 0, "✅ Positive")
    .otherwise("❌ Negative")
)

# Add processing timestamp
df_summary = df_summary.withColumn("updated_at", current_timestamp())

# Save summary gold table
gold_summary_table = get_table_path("gold", "stock_market_summary")

df_summary.write.format("delta").mode("overwrite").saveAsTable(gold_summary_table)

print(f"✅ Gold summary layer saved: {gold_summary_table}")
print("\n📊 Stock Market Performance Summary:")
display(df_summary.orderBy(col("total_return_pct").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Wheel Deployment to Databricks
# MAGIC
# MAGIC Now that we've validated our pipeline logic, let's deploy the wheel for production use.
# MAGIC
# MAGIC ## Step 1: Create Unity Catalog Volume (if not exists)

# COMMAND ----------

# Create volume for production libraries
# This volume will be created in the user's personal schema to ensure write access.
volume_prod = "production_libraries"
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {CATALOG}.{USER_SCHEMA}.{volume_prod}
COMMENT 'Production Python wheels for stock market pipeline'
""")

volume_prod_path = f"/Volumes/{CATALOG}/{USER_SCHEMA}/{volume_prod}/"
print(f"✅ Volume created: {CATALOG}.{USER_SCHEMA}.{volume_prod}")

# List current files
print(f"\n📦 Current files in {volume_prod_path}:")
try:
    files = dbutils.fs.ls(volume_prod_path)
    if not files:
        print("   (Volume is empty)")
    for file in files:
        print(f"   {file.name}")
except Exception as e:
    print(f"   (Volume is empty or newly created)")

# COMMAND ----------

# MAGIC %md
## Step 2: Upload Wheel to Volume
# MAGIC %md
# MAGIC
# MAGIC**Local Development Workflow**:
# MAGIC %md
# MAGIC Use the `volume_path` variable printed in the previous step as your destination.
# MAGIC
# MAGIC ```bash
# After building the wheel locally with: poetry build
# Upload to Databricks using CLI.
# IMPORTANT: Replace the destination path with the `volume_path` from the previous command's output.
# MAGIC
# MAGIC databricks fs cp \
# MAGIC  dist/stock_market_utils-1.0.0-py3-none-any.whl \
# MAGIC  /Volumes/<your_catalog>/<your_schema>/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl
# MAGIC
# Verify upload
# MAGIC databricks fs ls /Volumes/<your_catalog>/<your_schema>/production_libraries/
# MAGIC ```
# MAGIC
# MAGIC **Alternative: UI Upload**:
# MAGIC 1. Navigate to Catalog Explorer
# MAGIC 2. Browse to your personal schema: `<your_catalog>` → `<your_schema>` → `production_libraries`
# MAGIC 3. Click **Upload**
# MAGIC 4. Select `stock_market_utils-1.0.0-py3-none-any.whl` from your local `dist/` directory
# MAGIC
# MAGIC **Result**: The wheel is now available in your personal volume, ready for use in jobs.
# MAGIC
# MAGIC
# MAGIC ```bash
# After building the wheel locally with: poetry build
# Upload to Databricks using CLI.
# IMPORTANT: Replace the destination path with the `volume_path` from the previous command's output.
# MAGIC
# MAGIC databricks fs cp \
# MAGIC  dist/stock_market_utils-1.0.0-py3-none-any.whl \
# MAGIC  /Volumes/<your_catalog>/<your_schema>/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl
# MAGIC
# Verify upload
# MAGIC databricks fs ls /Volumes/<your_catalog>/<your_schema>/production_libraries/
# MAGIC ```
# MAGIC
# MAGIC **Alternative: UI Upload**:
# MAGIC 1. Navigate to Catalog Explorer
# MAGIC 2. Browse to your personal schema: `<your_catalog>` → `<your_schema>` → `production_libraries`
# MAGIC 3. Click **Upload**
# MAGIC 4. Select `stock_market_utils-1.0.0-py3-none-any.whl` from your local `dist/` directory
# MAGIC
# MAGIC **Result**: The wheel is now available in your personal volume, ready for use in jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4: Job Orchestration - UI Approach
# MAGIC
# MAGIC ## Creating a Production Stock Market Pipeline Job (UI)
# MAGIC
# MAGIC Follow these steps to create a multi-task job in the Databricks UI:
# MAGIC
# MAGIC ### Step 1: Navigate to Workflows
# MAGIC 1. Click **Workflows** in the left sidebar
# MAGIC 2. Click **Create Job** button
# MAGIC 3. Enter job name: `Stock Market Pipeline - Production`
# MAGIC
# MAGIC ### Step 2: Create Bronze Task (Data Ingestion)
# MAGIC 1. Click **Add task** → **Notebook task**
# MAGIC 2. **Task name**: `bronze_ingest_stock_data`
# MAGIC 3. **Type**: Notebook
# MAGIC 4. **Source**: Workspace
# MAGIC 5. **Notebook path**: `/Workspace/course/notebooks/05_week/21_stock_market_wheel_deployment`
# MAGIC 6. **Run only selected cell ranges**: `Bronze Layer` (cells 18-23)
# MAGIC 7. **Cluster**:
# MAGIC    - **New job cluster** (recommended)
# MAGIC    - **Runtime**: 14.3 LTS or higher
# MAGIC    - **Node type**: Standard_DS3_v2 (or equivalent)
# MAGIC    - **Workers**: 2
# MAGIC 8. **Libraries**:
# MAGIC    - Click **Add** → **Python Whl**
# MAGIC    - **Path**: Use the path to the wheel in your personal volume, e.g., `/Volumes/<your_catalog>/<your_schema>/production_libraries/stock_market_utils-1.0.0-py3-none-any.whl`
# MAGIC    - Click **Add** → **PyPI**
# MAGIC    - Package: `yfinance`
# MAGIC 9. **Parameters**:
# MAGIC    - `symbols`: `AAPL,GOOGL,MSFT,AMZN,NVDA`
# MAGIC    - `start_date`: `2024-01-01`
# MAGIC    - `end_date`: `2024-12-31`
# MAGIC 10. **Timeout**: 3600 seconds (1 hour)
# MAGIC 11. **Retries**: 2
# MAGIC 12. Click **Create**
# MAGIC
# MAGIC ### Step 3: Create Silver Task (Returns Calculation)
# MAGIC 1. Click **Add task** (under the Bronze task)
# MAGIC 2. **Task name**: `silver_calculate_returns`
# MAGIC 3. **Type**: Notebook
# MAGIC 4. **Notebook path**: Same as Bronze
# MAGIC 5. **Run only selected cell ranges**: `Silver Layer` (cells 26-32)
# MAGIC 6. **Depends on**: `bronze_ingest_stock_data`
# MAGIC 7. **Cluster**: Use same cluster as Bronze task
# MAGIC 8. **Libraries**: Same as Bronze task
# MAGIC 9. **Timeout**: 1800 seconds
# MAGIC 10. **Retries**: 2
# MAGIC 11. Click **Create**
# MAGIC
# MAGIC ### Step 4: Create Gold Task (Analytics Aggregation)
# MAGIC 1. Click **Add task** (under the Silver task)
# MAGIC 2. **Task name**: `gold_market_insights`
# MAGIC 3. **Type**: Notebook
# MAGIC 4. **Notebook path**: Same as previous tasks
# MAGIC 5. **Run only selected cell ranges**: `Gold Layer` (cells 35-45)
# MAGIC 6. **Depends on**: `silver_calculate_returns`
# MAGIC 7. **Cluster**: Use same cluster
# MAGIC 8. **Libraries**: Same as previous tasks
# MAGIC 9. **Timeout**: 1800 seconds
# MAGIC 10. **Retries**: 1
# MAGIC 11. Click **Create**
# MAGIC
# MAGIC ### Step 5: Configure Job Settings
# MAGIC 1. Click **Job settings** (top right)
# MAGIC 2. **Schedule** (optional):
# MAGIC    - Click **Add schedule**
# MAGIC    - **Schedule type**: Scheduled
# MAGIC    - **Trigger type**: Cron
# MAGIC    - **Cron expression**: `0 18 * * 1-5` (6 PM on weekdays, after market close)
# MAGIC    - **Timezone**: Your timezone
# MAGIC 3. **Email notifications**:
# MAGIC    - **On success**: Add your email
# MAGIC    - **On failure**: Add your email
# MAGIC 4. **Maximum concurrent runs**: 1 (prevent overlapping runs)
# MAGIC 5. Click **Save**
# MAGIC
# MAGIC ### Step 6: Run the Job
# MAGIC 1. Click **Run now** button
# MAGIC 2. Monitor execution in real-time:
# MAGIC    - View task progress in DAG visualization
# MAGIC    - Click each task to see logs
# MAGIC    - Monitor cluster auto-scaling
# MAGIC 3. After completion:
# MAGIC    - Review run duration
# MAGIC    - Check output tables
# MAGIC    - Verify data quality
# MAGIC
# MAGIC ### Step 7: Verify Results
# MAGIC 1. Navigate to **Catalog Explorer**
# MAGIC 2. Browse to your schema: `databricks_course.{your_schema}`
# MAGIC 3. Verify tables exist:
# MAGIC    - `bronze_stock_market_raw`
# MAGIC    - `silver_stock_market_returns`
# MAGIC    - `gold_stock_market_detailed_analytics`
# MAGIC    - `gold_stock_market_summary`
# MAGIC 4. Query summary table:
# MAGIC    ```sql
# MAGIC    SELECT * FROM databricks_course.{your_schema}.gold_stock_market_summary
# MAGIC    ORDER BY total_return_pct DESC;
# MAGIC    ```

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