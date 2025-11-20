# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced: Stock Market Analyzer - Streamlit App
# MAGIC
# MAGIC ## 📚 Table of Contents
# MAGIC
# MAGIC 1. [Getting Started](#getting-started) - **START HERE if you're new**
# MAGIC 2. [Prerequisites Checklist](#prerequisites) - What you need before starting
# MAGIC 3. [Understanding the App](#understanding) - What this app does
# MAGIC 4. [Step-by-Step Setup](#setup) - How to run it
# MAGIC 5. [App Code with Explanations](#code) - The actual Streamlit application
# MAGIC 6. [Testing Guide](#testing) - How to verify it works
# MAGIC 7. [Deployment Guide](#deployment) - How to publish it
# MAGIC 8. [Troubleshooting](#troubleshooting) - Common issues and solutions
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Getting Started
# MAGIC
# MAGIC ## What is This Notebook?
# MAGIC
# MAGIC This notebook teaches you how to build an **interactive data application** using Streamlit on Databricks. Think of it as transforming your data analysis into a user-friendly web app that anyone can use - no coding required for end users!
# MAGIC
# MAGIC ## What Will You Learn?
# MAGIC
# MAGIC ✅ **Build Interactive Apps**: Create web applications with charts, filters, and user inputs
# MAGIC ✅ **Connect to Unity Catalog**: Query your gold layer tables from the app
# MAGIC ✅ **Visualize Data**: Use Plotly for professional, interactive charts
# MAGIC ✅ **Deploy Apps**: Publish your app so others can access it via URL
# MAGIC ✅ **Real-World Skills**: This pattern works for sales dashboards, marketing analytics, operations monitoring, etc.
# MAGIC
# MAGIC ## Who is This For?
# MAGIC
# MAGIC - **Data Engineers**: Learn to build self-service analytics tools
# MAGIC - **Business Analysts**: Create interactive reports without asking IT
# MAGIC - **ML Engineers**: Build model demos and prediction interfaces
# MAGIC - **Anyone**: Who wants to share their data insights through a web app
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # ✅ Prerequisites Checklist
# MAGIC
# MAGIC Before you start, make sure you have completed these steps:
# MAGIC
# MAGIC ## Required (Must Have)
# MAGIC
# MAGIC - [ ] **Completed Notebook 21**: This app uses gold tables from the Stock Market Wheel Deployment notebook
# MAGIC   - You should have these tables in your schema:
# MAGIC     - `gold_stock_market_summary`
# MAGIC     - `gold_stock_market_detailed_analytics`
# MAGIC
# MAGIC - [ ] **Know Your Schema Name**: You need to know your personal schema name
# MAGIC   - Format: `firstname_lastname` (e.g., `chanukya_pekala`)
# MAGIC   - Check with: `SHOW SCHEMAS IN databricks_course`
# MAGIC
# MAGIC - [ ] **Have Data in Gold Tables**: Verify you have data
# MAGIC   - Run: `SELECT COUNT(*) FROM databricks_course.your_schema.gold_stock_market_summary`
# MAGIC   - Should return 5 (for 5 stocks: AAPL, GOOGL, MSFT, AMZN, NVDA)
# MAGIC
# MAGIC ## Nice to Have (Helpful but Optional)
# MAGIC
# MAGIC - [ ] **Basic Python Knowledge**: Understanding of variables, functions, and data structures
# MAGIC - [ ] **Familiarity with Pandas**: Experience with DataFrames
# MAGIC - [ ] **Read Notebook 01 (Apps Guide)**: Understanding of Databricks Apps concepts
# MAGIC
# MAGIC ## Quick Verification
# MAGIC
# MAGIC Run this cell to check if your data is ready:
# MAGIC
# MAGIC ```python
# MAGIC # Replace with your schema name
# MAGIC schema_name = "your_schema_name"
# MAGIC
# MAGIC # Check if tables exist
# MAGIC summary_count = spark.sql(f"SELECT COUNT(*) FROM databricks_course.{schema_name}.gold_stock_market_summary").collect()[0][0]
# MAGIC details_count = spark.sql(f"SELECT COUNT(*) FROM databricks_course.{schema_name}.gold_stock_market_detailed_analytics").collect()[0][0]
# MAGIC
# MAGIC print(f"✅ Summary table has {summary_count} stocks")
# MAGIC print(f"✅ Details table has {details_count} daily records")
# MAGIC print(f"\nYou're ready to proceed!")
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎯 Understanding the App
# MAGIC
# MAGIC ## What Does This App Do?
# MAGIC
# MAGIC This Streamlit application provides an **interactive stock market dashboard** with four main features:
# MAGIC
# MAGIC ### 1. 📊 Market Overview
# MAGIC **What it shows**: High-level metrics and performance comparison
# MAGIC **User sees**:
# MAGIC - Total number of stocks analyzed
# MAGIC - Average return across all stocks
# MAGIC - Best performing stock
# MAGIC - Average volatility (risk measure)
# MAGIC - Bar chart comparing all stocks' returns
# MAGIC
# MAGIC **Why it's useful**: Gives a quick snapshot of overall market performance
# MAGIC
# MAGIC ### 2. ⚖️ Risk-Return Analysis
# MAGIC **What it shows**: Interactive scatter plot showing risk vs. reward
# MAGIC **User sees**:
# MAGIC - Each stock as a bubble on the chart
# MAGIC - X-axis = Volatility (risk)
# MAGIC - Y-axis = Total return (reward)
# MAGIC - Bubble size = Trading volume
# MAGIC - Color = Performance tier (high/good/positive/negative)
# MAGIC
# MAGIC **Why it's useful**: Helps identify which stocks have good returns with lower risk (ideal quadrant is top-left: high return, low volatility)
# MAGIC
# MAGIC ### 3. 🔍 Detailed Stock Analysis
# MAGIC **What it shows**: Deep dive into a single stock's performance
# MAGIC **User sees**:
# MAGIC - Price history over time
# MAGIC - Trading volume chart
# MAGIC - Daily returns distribution (how often the stock goes up/down)
# MAGIC - Cumulative returns (if you invested on day 1, what's your total gain?)
# MAGIC - 30-day rolling volatility (is risk increasing or decreasing?)
# MAGIC - Raw data table
# MAGIC
# MAGIC **Why it's useful**: Understand a stock's behavior patterns before investing
# MAGIC
# MAGIC ### 4. 💼 Portfolio Simulator
# MAGIC **What it shows**: Build a hypothetical portfolio and see its performance
# MAGIC **User can**:
# MAGIC - Select multiple stocks
# MAGIC - Set number of shares for each
# MAGIC - See total investment amount
# MAGIC - See total return (profit/loss)
# MAGIC - Visualize portfolio composition (pie chart)
# MAGIC
# MAGIC **Why it's useful**: Test different investment strategies without risking real money
# MAGIC
# MAGIC ## Data Source
# MAGIC
# MAGIC This app uses **gold layer tables** created in Notebook 21:
# MAGIC - `gold_stock_market_summary` - Aggregate performance metrics (1 row per stock)
# MAGIC - `gold_stock_market_detailed_analytics` - Daily time series with volatility (1 row per stock per day)
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC User's Browser
# MAGIC      ↓ (views app)
# MAGIC Streamlit App
# MAGIC      ↓ (queries data)
# MAGIC Unity Catalog Gold Tables
# MAGIC      ↓ (reads from)
# MAGIC Delta Lake Storage
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 🛠️ Step-by-Step Setup
# MAGIC
# MAGIC Follow these steps to run the app. Each step is explained in detail.
# MAGIC
# MAGIC ## Step 1: Install Required Libraries
# MAGIC
# MAGIC **What we're doing**: Installing Streamlit (web app framework) and Plotly (charting library)
# MAGIC
# MAGIC **Why**: These libraries don't come pre-installed on Databricks clusters
# MAGIC
# MAGIC **What happens**:
# MAGIC - `%pip install` downloads and installs the packages
# MAGIC - `--quiet` suppresses verbose installation logs
# MAGIC - `dbutils.library.restartPython()` restarts Python to load new libraries
# MAGIC
# MAGIC **Run the cell below** ⬇️

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Your Data
# MAGIC
# MAGIC **What we're doing**: Checking that you have data in your gold tables
# MAGIC
# MAGIC **Why**: The app won't work without data
# MAGIC
# MAGIC **Instructions**:
# MAGIC 1. Replace `"your_schema_name"` with your actual schema name
# MAGIC 2. Run the cell
# MAGIC 3. You should see: "✅ Summary table has 5 stocks" and "✅ Details table has XXX daily records"
# MAGIC
# MAGIC If you see errors, go back to Notebook 21 and run it first!

# COMMAND ----------

# VERIFICATION CELL - Update schema name and run this
schema_name = "chanukya_pekala"  # ⚠️ CHANGE THIS to your schema name

try:
    # Check summary table
    summary_count = spark.sql(f"SELECT COUNT(*) FROM databricks_course.{schema_name}.gold_stock_market_summary").collect()[0][0]
    print(f"✅ Summary table has {summary_count} stocks")

    # Check details table
    details_count = spark.sql(f"SELECT COUNT(*) FROM databricks_course.{schema_name}.gold_stock_market_detailed_analytics").collect()[0][0]
    print(f"✅ Details table has {details_count} daily records")

    # Check which stocks we have
    stocks = spark.sql(f"SELECT symbol FROM databricks_course.{schema_name}.gold_stock_market_summary").toPandas()
    print(f"\n📊 Stocks available: {', '.join(stocks['symbol'].tolist())}")

    print(f"\n🎉 You're ready to proceed!")
except Exception as e:
    print(f"❌ Error: {str(e)}")
    print(f"\n⚠️ Troubleshooting steps:")
    print(f"1. Verify schema name is correct: {schema_name}")
    print(f"2. Run Notebook 21 to create the gold tables")
    print(f"3. Check table names match exactly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Understand the App Structure
# MAGIC
# MAGIC Before diving into the code, let's understand how the app is organized:
# MAGIC
# MAGIC ### Code Organization
# MAGIC
# MAGIC ```python
# MAGIC 1. Page Configuration
# MAGIC    ├── Set page title, icon, layout
# MAGIC    └── Configure sidebar behavior
# MAGIC
# MAGIC 2. Spark Session Setup
# MAGIC    ├── Connect to Databricks cluster
# MAGIC    └── Enable querying Unity Catalog
# MAGIC
# MAGIC 3. Data Loading Functions
# MAGIC    ├── load_stock_summary() - Get aggregate metrics
# MAGIC    ├── load_stock_details() - Get daily time series
# MAGIC    └── Use @st.cache_data for performance
# MAGIC
# MAGIC 4. App Layout
# MAGIC    ├── Title and description
# MAGIC    ├── Sidebar (configuration inputs)
# MAGIC    ├── Market Overview section
# MAGIC    ├── Performance Comparison charts
# MAGIC    ├── Risk-Return Analysis
# MAGIC    ├── Detailed Stock Analysis
# MAGIC    ├── Portfolio Simulator
# MAGIC    └── Footer
# MAGIC ```
# MAGIC
# MAGIC ### Key Streamlit Concepts Used
# MAGIC
# MAGIC | Concept | What It Does | Example |
# MAGIC |---------|--------------|---------|
# MAGIC | `st.title()` | Creates a large heading | `st.title("My App")` |
# MAGIC | `st.sidebar` | Adds elements to sidebar | `st.sidebar.text_input()` |
# MAGIC | `st.metric()` | Shows a metric card | `st.metric("Sales", "$100K")` |
# MAGIC | `st.selectbox()` | Creates dropdown menu | `st.selectbox("Pick", [1,2,3])` |
# MAGIC | `st.plotly_chart()` | Displays Plotly chart | `st.plotly_chart(fig)` |
# MAGIC | `st.columns()` | Creates side-by-side layout | `col1, col2 = st.columns(2)` |
# MAGIC | `@st.cache_data` | Caches function results | Prevents re-querying same data |
# MAGIC
# MAGIC ### Data Flow
# MAGIC
# MAGIC ```
# MAGIC User opens app
# MAGIC    ↓
# MAGIC User enters catalog/schema in sidebar
# MAGIC    ↓
# MAGIC App queries Unity Catalog tables
# MAGIC    ↓
# MAGIC Data is cached (for 1 hour)
# MAGIC    ↓
# MAGIC App renders visualizations
# MAGIC    ↓
# MAGIC User interacts (clicks, selects, filters)
# MAGIC    ↓
# MAGIC App updates without re-querying data
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # 💻 App Code with Detailed Explanations
# MAGIC
# MAGIC Now let's look at the actual Streamlit code. Each section is heavily commented to explain what's happening.
# MAGIC
# MAGIC ## Installation Cell
# MAGIC
# MAGIC This cell installs the required Python libraries and restarts the Python interpreter to load them.

# COMMAND ----------

# MAGIC %md
# MAGIC # Streamlit App Code
# MAGIC
# MAGIC The following cell contains the complete Streamlit application.
# MAGIC You can run this cell to test the app in a notebook, or deploy it as a Databricks App.

# COMMAND ----------

# Install Streamlit (if not already available)
%pip install streamlit plotly --quiet
dbutils.library.restartPython()

# COMMAND ----------

# =====================================================================
# PART 1: IMPORTS AND INITIAL SETUP
# =====================================================================
# Import all required libraries for the Streamlit app
import streamlit as st          # Web app framework
import pandas as pd             # Data manipulation
import plotly.express as px     # High-level charting (bar, scatter, line charts)
import plotly.graph_objects as go  # Low-level charting (custom visualizations)
from plotly.subplots import make_subplots  # Create multi-chart layouts
from pyspark.sql import SparkSession       # Connect to Databricks/Spark
from pyspark.sql import functions as F     # Spark SQL functions
from datetime import datetime, timedelta   # Date/time handling

# =====================================================================
# PART 2: PAGE CONFIGURATION
# =====================================================================
# Configure the Streamlit app's appearance and behavior
# This MUST be the first Streamlit command in the script
st.set_page_config(
    page_title="Stock Market Analyzer",    # Browser tab title
    page_icon="📈",                        # Browser tab icon
    layout="wide",                         # Use full screen width (vs. "centered")
    initial_sidebar_state="expanded"       # Show sidebar by default
)

# =====================================================================
# PART 3: SPARK SESSION SETUP
# =====================================================================
# Initialize connection to Databricks cluster to query Unity Catalog tables

@st.cache_resource  # Cache this connection across all users (singleton pattern)
def get_spark():
    """
    Get or create Spark session.

    Why @st.cache_resource?
    - This is a connection/resource that should be shared across users
    - Only created once and reused for all sessions
    - Different from @st.cache_data which is for data caching
    """
    return SparkSession.builder.getOrCreate()

# Create the Spark session (runs once, then cached)
spark = get_spark()

# =====================================================================
# PART 3B: AUTO-DETECT USER SCHEMA
# =====================================================================
# Automatically determine the user's schema name from their email
# This matches the pattern used in user_schema_setup.py

@st.cache_data  # Cache the user's schema detection
def get_user_schema():
    """
    Auto-detect the current user's schema name.

    This uses the same logic as ../utils/user_schema_setup.py:
    - Gets current user's email from Databricks
    - Extracts username before @ symbol
    - Replaces special characters with underscores
    - Returns lowercase schema name

    Example: chanukya.pekala@gmail.com → chanukya_pekala
    """
    import re
    try:
        # Get current user from Databricks context
        user_email = spark.sql("SELECT current_user()").collect()[0][0]

        # Extract schema name from email (same logic as user_schema_setup.py)
        user_schema = re.sub(r'[^a-zA-Z0-9_]', '_', user_email.split('@')[0]).lower()

        return user_schema, user_email
    except Exception as e:
        # Fallback if auto-detection fails
        st.warning(f"⚠️ Could not auto-detect user schema: {str(e)}")
        return None, None

# =====================================================================
# PART 4: DATA LOADING FUNCTIONS
# =====================================================================
# Functions to query Unity Catalog gold tables and convert to Pandas

@st.cache_data(ttl=3600)  # Cache for 1 hour (3600 seconds)
def load_stock_summary(catalog: str, schema: str):
    """
    Load stock market summary from gold layer.

    Returns aggregate metrics for all stocks:
    - symbol: Stock ticker (AAPL, GOOGL, etc.)
    - total_return_pct: Overall return percentage
    - avg_daily_return: Average daily return
    - volatility: Risk measure (standard deviation of returns)
    - performance_tier: Categorization (High/Good/Positive/Negative)

    Why @st.cache_data with ttl=3600?
    - Avoids re-querying Unity Catalog on every app interaction
    - Data refreshes every hour automatically
    - Improves app performance dramatically
    """
    table_name = f"{catalog}.{schema}.gold_stock_market_summary"
    try:
        # Query Unity Catalog table using Spark
        df_spark = spark.table(table_name)

        # Convert to Pandas for easier manipulation in Streamlit
        # (Streamlit and Plotly work better with Pandas than PySpark)
        df_pandas = df_spark.toPandas()

        return df_pandas
    except Exception as e:
        # User-friendly error handling
        st.error(f"Failed to load summary data: {str(e)}")
        st.info(f"Attempted to load: {table_name}")
        st.info("Please ensure you've run Notebook 21 to create the gold tables.")

        # Return empty DataFrame instead of crashing the app
        return pd.DataFrame()

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_stock_details(catalog: str, schema: str, symbol: str = None):
    """
    Load detailed stock data from gold layer.

    Returns daily time series data:
    - date: Trading date
    - close: Closing price
    - volume: Trading volume
    - daily_return: Daily return percentage
    - cumulative_return: Cumulative return from start
    - volatility_30d: 30-day rolling volatility

    Parameters:
    - symbol: Optional filter for specific stock (e.g., "AAPL")
    """
    table_name = f"{catalog}.{schema}.gold_stock_market_detailed_analytics"
    try:
        # Query Unity Catalog table
        df_spark = spark.table(table_name)

        # Filter for specific symbol if provided
        if symbol:
            df_spark = df_spark.filter(F.col("symbol") == symbol)

        # Convert to Pandas
        df_pandas = df_spark.toPandas()
        return df_pandas
    except Exception as e:
        st.error(f"Failed to load detailed data: {str(e)}")
        return pd.DataFrame()

# =====================================================================
# MAIN APP
# =====================================================================

# Title and description
st.title("📈 Stock Market Analyzer")
st.markdown("""
This application provides interactive analysis of stock market data from the gold layer tables.
Data is sourced from Yahoo Finance and processed through a medallion architecture pipeline.
""")

# Sidebar - Configuration
st.sidebar.title("⚙️ Configuration")

# Auto-detect user schema
detected_schema, detected_email = get_user_schema()

# Catalog and schema selection with auto-detection
catalog = st.sidebar.text_input(
    "Unity Catalog",
    value="databricks_course",
    help="Enter your catalog name"
)

# Use detected schema as default, or fallback to empty string
default_schema = detected_schema if detected_schema else ""
schema = st.sidebar.text_input(
    "Schema",
    value=default_schema,
    help="Auto-detected from your email. Change if needed."
)

# Show detected user info
if detected_email:
    st.sidebar.info(f"👤 Detected user: {detected_email}\n\n📁 Auto-detected schema: `{detected_schema}`")

# Load data button
if st.sidebar.button("🔄 Reload Data", help="Refresh data from Unity Catalog"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### 📊 Data Source")
st.sidebar.code(f"{catalog}.{schema}.gold_*", language="sql")

# Load summary data
df_summary = load_stock_summary(catalog, schema)

if df_summary.empty:
    st.warning("⚠️ No data available. Please check your configuration and ensure gold tables exist.")
    st.stop()

# =====================================================================
# OVERVIEW TAB
# =====================================================================

st.markdown("---")

# Key metrics at the top
st.subheader("📊 Market Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stocks = len(df_summary)
    st.metric("Total Stocks Analyzed", total_stocks)

with col2:
    avg_return = df_summary["total_return_pct"].mean()
    st.metric("Average Return", f"{avg_return:.2f}%", delta=f"{avg_return:.2f}%")

with col3:
    best_stock = df_summary.loc[df_summary["total_return_pct"].idxmax()]
    st.metric(
        "Best Performer",
        best_stock["symbol"],
        delta=f"+{best_stock['total_return_pct']:.2f}%",
        delta_color="normal"
    )

with col4:
    avg_volatility = df_summary["volatility"].mean()
    st.metric("Avg Volatility", f"{avg_volatility:.4f}")

# =====================================================================
# PERFORMANCE COMPARISON
# =====================================================================

st.markdown("---")
st.subheader("🏆 Stock Performance Comparison")

# Sort by return
df_sorted = df_summary.sort_values("total_return_pct", ascending=False)

# Create bar chart for returns
fig_returns = px.bar(
    df_sorted,
    x="symbol",
    y="total_return_pct",
    title="Total Returns by Stock",
    labels={"total_return_pct": "Return (%)", "symbol": "Stock Symbol"},
    color="total_return_pct",
    color_continuous_scale=["red", "yellow", "green"],
    text="total_return_pct"
)

fig_returns.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
fig_returns.update_layout(height=400, showlegend=False)

st.plotly_chart(fig_returns, use_container_width=True)

# =====================================================================
# RISK-RETURN ANALYSIS
# =====================================================================

st.markdown("---")
st.subheader("⚖️ Risk-Return Analysis")

col1, col2 = st.columns([2, 1])

with col1:
    # Scatter plot: Risk vs Return
    fig_risk_return = px.scatter(
        df_summary,
        x="volatility",
        y="total_return_pct",
        size="avg_daily_volume",
        color="performance_tier",
        hover_data=["symbol", "avg_daily_return"],
        title="Risk-Return Profile",
        labels={
            "volatility": "Volatility (Risk)",
            "total_return_pct": "Total Return (%)",
            "avg_daily_volume": "Avg Daily Volume"
        },
        text="symbol"
    )

    fig_risk_return.update_traces(textposition='top center')
    fig_risk_return.update_layout(height=500)

    st.plotly_chart(fig_risk_return, use_container_width=True)

with col2:
    st.markdown("### 📝 Interpretation")
    st.markdown("""
    **Ideal Quadrant**: High return, low volatility (top-left)

    **Performance Tiers**:
    - 🔥 High Performer: >50% return
    - ⭐ Good Performer: 20-50% return
    - ✅ Positive: 0-20% return
    - ❌ Negative: <0% return

    **Risk Levels**:
    - Low: Volatility < 1.0
    - Medium: Volatility 1.0-2.0
    - High: Volatility > 2.0
    """)

# =====================================================================
# DETAILED STOCK ANALYSIS
# =====================================================================

st.markdown("---")
st.subheader("🔍 Detailed Stock Analysis")

# Stock selector
selected_symbol = st.selectbox(
    "Select a stock to analyze in detail",
    options=df_summary["symbol"].tolist(),
    index=0
)

# Load detailed data for selected stock
df_details = load_stock_details(catalog, schema, selected_symbol)

if not df_details.empty:
    # Stock summary card
    stock_info = df_summary[df_summary["symbol"] == selected_symbol].iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Return",
            f"{stock_info['total_return_pct']:.2f}%",
            delta=f"{stock_info['total_return_pct']:.2f}%"
        )

    with col2:
        st.metric(
            "Avg Daily Return",
            f"{stock_info['avg_daily_return']:.4f}%"
        )

    with col3:
        st.metric(
            "Volatility",
            f"{stock_info['volatility']:.4f}"
        )

    with col4:
        st.metric(
            "Trading Days",
            f"{int(stock_info['trading_days'])}"
        )

    # Price history chart
    st.markdown("### 📈 Price History")

    # Ensure date column is datetime
    df_details['date'] = pd.to_datetime(df_details['date'])
    df_details = df_details.sort_values('date')

    # Create subplots: Price and Volume
    fig_detailed = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('Stock Price', 'Daily Volume'),
        row_heights=[0.7, 0.3]
    )

    # Price trace
    fig_detailed.add_trace(
        go.Scatter(
            x=df_details['date'],
            y=df_details['close'],
            mode='lines',
            name='Close Price',
            line=dict(color='#1f77b4', width=2)
        ),
        row=1, col=1
    )

    # Volume trace
    fig_detailed.add_trace(
        go.Bar(
            x=df_details['date'],
            y=df_details['volume'],
            name='Volume',
            marker=dict(color='#7f7f7f', opacity=0.5)
        ),
        row=2, col=1
    )

    fig_detailed.update_xaxes(title_text="Date", row=2, col=1)
    fig_detailed.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig_detailed.update_yaxes(title_text="Volume", row=2, col=1)

    fig_detailed.update_layout(height=600, showlegend=True)

    st.plotly_chart(fig_detailed, use_container_width=True)

    # Returns analysis
    st.markdown("### 📊 Returns Analysis")

    col1, col2 = st.columns(2)

    with col1:
        # Daily returns distribution
        fig_daily_returns = px.histogram(
            df_details,
            x="daily_return",
            title="Daily Returns Distribution",
            labels={"daily_return": "Daily Return (%)"},
            nbins=30,
            color_discrete_sequence=["#2ca02c"]
        )
        fig_daily_returns.update_layout(height=350)
        st.plotly_chart(fig_daily_returns, use_container_width=True)

    with col2:
        # Cumulative returns over time
        fig_cumulative = px.line(
            df_details,
            x="date",
            y="cumulative_return",
            title="Cumulative Returns Over Time",
            labels={"cumulative_return": "Cumulative Return (%)", "date": "Date"}
        )
        fig_cumulative.update_layout(height=350)
        st.plotly_chart(fig_cumulative, use_container_width=True)

    # Volatility analysis
    if "volatility_30d" in df_details.columns:
        st.markdown("### 📉 Volatility Analysis (30-Day Rolling)")

        fig_volatility = px.line(
            df_details,
            x="date",
            y="volatility_30d",
            title="30-Day Rolling Volatility",
            labels={"volatility_30d": "Volatility", "date": "Date"},
            color_discrete_sequence=["#d62728"]
        )
        fig_volatility.update_layout(height=350)
        st.plotly_chart(fig_volatility, use_container_width=True)

    # Raw data table
    with st.expander("📋 View Raw Data"):
        st.dataframe(
            df_details[[
                "date", "close", "daily_return", "cumulative_return",
                "volatility_30d", "volume"
            ]].sort_values("date", ascending=False),
            use_container_width=True
        )

else:
    st.warning(f"No detailed data available for {selected_symbol}")

# =====================================================================
# PORTFOLIO SIMULATOR (BONUS FEATURE)
# =====================================================================

st.markdown("---")
st.subheader("💼 Portfolio Simulator")

st.markdown("Build a hypothetical portfolio and see its performance.")

# Portfolio builder
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("#### Select Stocks and Shares")

    portfolio = {}
    for _, stock in df_summary.iterrows():
        col_a, col_b = st.columns([3, 1])
        with col_a:
            st.text(f"{stock['symbol']} (${stock['avg_close']:.2f})")
        with col_b:
            shares = st.number_input(
                "Shares",
                min_value=0,
                max_value=1000,
                value=0,
                step=10,
                key=f"shares_{stock['symbol']}",
                label_visibility="collapsed"
            )
            if shares > 0:
                portfolio[stock['symbol']] = shares

with col2:
    st.markdown("#### Portfolio Summary")

    if portfolio:
        total_value = 0
        total_return = 0

        for symbol, shares in portfolio.items():
            stock_data = df_summary[df_summary["symbol"] == symbol].iloc[0]
            value = shares * stock_data["avg_close"]
            ret = value * (stock_data["total_return_pct"] / 100)

            total_value += value
            total_return += ret

        st.metric("Total Investment", f"${total_value:,.2f}")
        st.metric(
            "Total Return",
            f"${total_return:,.2f}",
            delta=f"{(total_return / total_value) * 100:.2f}%"
        )

        # Portfolio composition
        portfolio_df = pd.DataFrame([
            {
                "Stock": symbol,
                "Shares": shares,
                "Value": shares * df_summary[df_summary["symbol"] == symbol]["avg_close"].values[0]
            }
            for symbol, shares in portfolio.items()
        ])

        fig_portfolio = px.pie(
            portfolio_df,
            values="Value",
            names="Stock",
            title="Portfolio Composition"
        )
        st.plotly_chart(fig_portfolio, use_container_width=True)
    else:
        st.info("Add stocks to your portfolio using the controls on the left.")

# =====================================================================
# FOOTER
# =====================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 20px;'>
    <p>📊 Data sourced from Yahoo Finance | 🏗️ Built with Streamlit on Databricks</p>
    <p>🔄 Data refreshed from Unity Catalog gold layer tables</p>
</div>
""", unsafe_allow_html=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Testing the App
# MAGIC
# MAGIC To test the app in this notebook:
# MAGIC
# MAGIC 1. Ensure you've run **Notebook 21** to create the gold tables
# MAGIC 2. Update the `catalog` and `schema` values in the sidebar to match your configuration
# MAGIC 3. Click "Reload Data" to fetch from your tables
# MAGIC 4. Explore the different visualizations and features
# MAGIC
# MAGIC ## Expected Features
# MAGIC
# MAGIC ✅ **Market Overview**:
# MAGIC - Key metrics (total stocks, average return, best performer)
# MAGIC - Performance comparison bar chart
# MAGIC
# MAGIC ✅ **Risk-Return Analysis**:
# MAGIC - Scatter plot showing risk vs. return trade-off
# MAGIC - Color-coded by performance tier
# MAGIC - Size by trading volume
# MAGIC
# MAGIC ✅ **Detailed Stock Analysis**:
# MAGIC - Price history with volume
# MAGIC - Daily returns distribution
# MAGIC - Cumulative returns over time
# MAGIC - 30-day rolling volatility
# MAGIC - Raw data table
# MAGIC
# MAGIC ✅ **Portfolio Simulator**:
# MAGIC - Build hypothetical portfolios
# MAGIC - Calculate total value and returns
# MAGIC - Visualize portfolio composition
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC After testing successfully:
# MAGIC
# MAGIC 1. **Deploy as Databricks App**:
# MAGIC    - Click **Publish** → **Databricks App**
# MAGIC    - Configure name: "Stock Market Analyzer"
# MAGIC    - Deploy and share URL with stakeholders
# MAGIC
# MAGIC 2. **Customize for Your Use Case**:
# MAGIC    - Add more stocks
# MAGIC    - Include different time periods
# MAGIC    - Add technical indicators
# MAGIC    - Implement alerts/notifications
# MAGIC
# MAGIC 3. **Integrate with Workflows**:
# MAGIC    - Schedule data refresh jobs
# MAGIC    - Trigger app updates on new data
# MAGIC    - Send automated reports

# COMMAND ----------

# MAGIC %md
# MAGIC # Deployment Guide
# MAGIC
# MAGIC ## Method 1: Deploy from Notebook
# MAGIC
# MAGIC 1. **Prepare the notebook**:
# MAGIC    - Ensure all cells run successfully
# MAGIC    - Test with your data
# MAGIC
# MAGIC 2. **Publish as App**:
# MAGIC    - Click **Publish** button (top right)
# MAGIC    - Select **Databricks App**
# MAGIC    - Enter app name: `Stock Market Analyzer`
# MAGIC    - Click **Deploy**
# MAGIC
# MAGIC 3. **Configure App**:
# MAGIC    - Set default catalog and schema parameters
# MAGIC    - Configure access permissions
# MAGIC    - Enable/disable public access
# MAGIC
# MAGIC 4. **Share**:
# MAGIC    - Copy app URL
# MAGIC    - Share with stakeholders
# MAGIC    - Monitor usage metrics
# MAGIC
# MAGIC ## Method 2: Deploy from Git Repository
# MAGIC
# MAGIC ### Step 1: Create Repository Structure
# MAGIC
# MAGIC ```
# MAGIC stock-market-analyzer/
# MAGIC ├── app.py                    # Main Streamlit app (copy from cell above)
# MAGIC ├── requirements.txt          # Dependencies
# MAGIC ├── README.md                 # Documentation
# MAGIC └── .gitignore
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Create requirements.txt
# MAGIC
# MAGIC ```text
# MAGIC streamlit>=1.28.0
# MAGIC plotly>=5.17.0
# MAGIC pandas>=2.1.0
# MAGIC pyspark>=3.5.0
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Extract Streamlit Code
# MAGIC
# MAGIC Copy the Streamlit code from the cell above to `app.py`:
# MAGIC
# MAGIC ```python
# MAGIC # app.py
# MAGIC import streamlit as st
# MAGIC import pandas as pd
# MAGIC # ... (rest of the code from the Streamlit cell)
# MAGIC ```
# MAGIC
# MAGIC ### Step 4: Push to Git
# MAGIC
# MAGIC ```bash
# MAGIC git init
# MAGIC git add .
# MAGIC git commit -m "Initial commit: Stock market analyzer app"
# MAGIC git push origin main
# MAGIC ```
# MAGIC
# MAGIC ### Step 5: Deploy via Databricks UI
# MAGIC
# MAGIC 1. Navigate to **Apps** in Databricks
# MAGIC 2. Click **Create App**
# MAGIC 3. Select **Git repository** as source
# MAGIC 4. Connect your repository
# MAGIC 5. Select branch: `main`
# MAGIC 6. Set entry point: `app.py`
# MAGIC 7. Click **Deploy**
# MAGIC
# MAGIC ### Step 6: Configure Environment
# MAGIC
# MAGIC 1. Set environment variables (if needed):
# MAGIC    ```
# MAGIC    CATALOG=databricks_course
# MAGIC    SCHEMA=your_schema
# MAGIC    ```
# MAGIC
# MAGIC 2. Configure compute:
# MAGIC    - Select Serverless (recommended)
# MAGIC    - Or specify cluster size
# MAGIC
# MAGIC 3. Set permissions:
# MAGIC    - Viewer access for stakeholders
# MAGIC    - Editor access for maintainers
# MAGIC
# MAGIC ## Method 3: Deploy via SDK (Automation)
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service import apps
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # Create app
# MAGIC app = w.apps.create(
# MAGIC     name="stock-market-analyzer",
# MAGIC     source_code_path="/Workspace/Repos/username/stock-market-analyzer",
# MAGIC     description="Interactive stock market analysis dashboard"
# MAGIC )
# MAGIC
# MAGIC # Deploy app
# MAGIC deployment = w.apps.deploy(
# MAGIC     app_name="stock-market-analyzer"
# MAGIC )
# MAGIC
# MAGIC print(f"App deployed: {deployment.url}")
# MAGIC ```
# MAGIC
# MAGIC ## Post-Deployment
# MAGIC
# MAGIC ### Monitoring
# MAGIC
# MAGIC - Check app logs for errors
# MAGIC - Monitor usage metrics
# MAGIC - Track query performance
# MAGIC - Review user feedback
# MAGIC
# MAGIC ### Maintenance
# MAGIC
# MAGIC - Update data refreshes
# MAGIC - Add new features
# MAGIC - Fix bugs
# MAGIC - Optimize performance
# MAGIC
# MAGIC ### Scaling
# MAGIC
# MAGIC - Increase compute resources if needed
# MAGIC - Optimize caching strategies
# MAGIC - Add more data sources
# MAGIC - Implement user segmentation

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔧 Troubleshooting Guide
# MAGIC
# MAGIC ## Common Issues and Solutions
# MAGIC
# MAGIC ### Issue 1: "Failed to load summary data: Table or view not found"
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Error message: `Table or view not found: databricks_course.your_schema.gold_stock_market_summary`
# MAGIC - App shows "⚠️ No data available"
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Gold tables haven't been created yet
# MAGIC - Schema name is incorrect
# MAGIC - You're looking in the wrong catalog
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Step 1: Verify your schema exists
# MAGIC SHOW SCHEMAS IN databricks_course;
# MAGIC
# MAGIC -- Step 2: Check if gold tables exist
# MAGIC SHOW TABLES IN databricks_course.your_schema;
# MAGIC
# MAGIC -- Step 3: If no gold tables, run Notebook 21 first
# MAGIC -- Go back and complete: 05_week/21_stock_market_wheel_deployment.py
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 2: "ModuleNotFoundError: No module named 'streamlit'"
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Error when running Streamlit import cell
# MAGIC - Python can't find streamlit package
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Streamlit not installed on cluster
# MAGIC - Python wasn't restarted after installation
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Run this cell again
# MAGIC %pip install streamlit plotly --quiet
# MAGIC dbutils.library.restartPython()
# MAGIC
# MAGIC # Wait for Python to restart, then run imports again
# MAGIC ```
# MAGIC
# MAGIC **Alternative:**
# MAGIC ```bash
# MAGIC # If above doesn't work, try without --quiet to see errors
# MAGIC %pip install streamlit plotly
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 3: "Schema name doesn't match my username"
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - You see error: `Table not found: databricks_course.chanukya_pekala.gold_*`
# MAGIC - But your name isn't "chanukya_pekala"
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Default schema name in sidebar is hardcoded
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # In the sidebar input, change the schema to YOUR schema name
# MAGIC # Format: firstname_lastname (e.g., john_smith)
# MAGIC
# MAGIC # Find your schema name:
# MAGIC SHOW SCHEMAS IN databricks_course;
# MAGIC
# MAGIC # Or programmatically:
# MAGIC user_email = spark.sql("SELECT current_user()").collect()[0][0]
# MAGIC user_schema = user_email.split('@')[0].replace('.', '_')
# MAGIC print(f"Your schema name: {user_schema}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 4: App loads but shows empty charts
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - App interface loads
# MAGIC - But all charts are empty or show "No data"
# MAGIC - No error messages
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Data exists but is malformed
# MAGIC - Missing required columns
# MAGIC - Gold tables are empty
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Step 1: Check if gold tables have data
# MAGIC SELECT COUNT(*) FROM databricks_course.your_schema.gold_stock_market_summary;
# MAGIC -- Should return 5 (for 5 stocks)
# MAGIC
# MAGIC -- Step 2: Verify columns exist
# MAGIC DESCRIBE TABLE databricks_course.your_schema.gold_stock_market_summary;
# MAGIC -- Should see: symbol, total_return_pct, avg_daily_return, volatility, etc.
# MAGIC
# MAGIC -- Step 3: Check a sample row
# MAGIC SELECT * FROM databricks_course.your_schema.gold_stock_market_summary LIMIT 1;
# MAGIC -- Should see actual stock data (AAPL, GOOGL, etc.)
# MAGIC ```
# MAGIC
# MAGIC **If tables are empty:**
# MAGIC - Re-run Notebook 21 completely
# MAGIC - Check for errors in bronze/silver layer processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 5: "KeyError: 'total_return_pct'" or similar column errors
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Python error: `KeyError: 'total_return_pct'`
# MAGIC - App crashes when trying to display metrics
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Gold table schema doesn't match expected format
# MAGIC - Notebook 21 was modified or incomplete
# MAGIC
# MAGIC **Solution:**
# MAGIC ```sql
# MAGIC -- Check actual columns in your table
# MAGIC DESCRIBE TABLE databricks_course.your_schema.gold_stock_market_summary;
# MAGIC
# MAGIC -- Expected columns:
# MAGIC -- - symbol
# MAGIC -- - total_return_pct
# MAGIC -- - avg_daily_return
# MAGIC -- - volatility
# MAGIC -- - performance_tier
# MAGIC -- - avg_close
# MAGIC -- - avg_daily_volume
# MAGIC -- - trading_days
# MAGIC ```
# MAGIC
# MAGIC **Fix:**
# MAGIC - If columns are missing, re-run Notebook 21
# MAGIC - Ensure you're using the unmodified version
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 6: Cache not refreshing / seeing old data
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Updated data in gold tables
# MAGIC - But app still shows old data
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Streamlit caching is working (as designed!)
# MAGIC - Data cached for 1 hour (ttl=3600)
# MAGIC
# MAGIC **Solution:**
# MAGIC ```
# MAGIC Click "🔄 Reload Data" button in sidebar
# MAGIC
# MAGIC OR
# MAGIC
# MAGIC Wait 1 hour for automatic cache expiration
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 7: "Connection error" or Spark session issues
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - Error: "Unable to connect to Spark"
# MAGIC - Spark session fails to initialize
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Cluster is not running
# MAGIC - Cluster restarted mid-execution
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Check cluster status in Databricks UI
# MAGIC # Restart cluster if needed
# MAGIC # Re-run the Spark session initialization cell
# MAGIC
# MAGIC @st.cache_resource
# MAGIC def get_spark():
# MAGIC     return SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC spark = get_spark()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 8: Deployment fails
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - "Publish → Databricks App" button grayed out
# MAGIC - Deployment starts but fails
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Databricks Apps not available on your workspace tier
# MAGIC - Insufficient permissions
# MAGIC - Code has errors
# MAGIC
# MAGIC **Solution:**
# MAGIC ```
# MAGIC 1. Verify Databricks Apps is enabled:
# MAGIC    - Check workspace edition (Premium/Enterprise required)
# MAGIC    - Contact workspace admin
# MAGIC
# MAGIC 2. Ensure all cells run without errors:
# MAGIC    - Run notebook top-to-bottom
# MAGIC    - Fix any errors before deploying
# MAGIC
# MAGIC 3. Check permissions:
# MAGIC    - Need "Can Manage" or "Can Run" on workspace
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 9: Performance is slow
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - App takes a long time to load
# MAGIC - Charts render slowly
# MAGIC - Feels sluggish
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - Large dataset
# MAGIC - Cache not working
# MAGIC - Complex queries
# MAGIC
# MAGIC **Solution:**
# MAGIC ```python
# MAGIC # Optimization 1: Verify caching is enabled
# MAGIC # Look for @st.cache_data decorators on load functions
# MAGIC
# MAGIC # Optimization 2: Reduce data volume
# MAGIC # Filter in Spark before converting to Pandas
# MAGIC df_spark = spark.table(table_name).limit(1000)  # Add limit
# MAGIC
# MAGIC # Optimization 3: Check cluster size
# MAGIC # Larger cluster = faster queries
# MAGIC # Databricks → Compute → Select larger instance type
# MAGIC
# MAGIC # Optimization 4: Optimize Delta tables
# MAGIC # OPTIMIZE databricks_course.your_schema.gold_stock_market_summary;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Issue 10: "This app is not accessible" after deployment
# MAGIC
# MAGIC **Symptoms:**
# MAGIC - App deployed successfully
# MAGIC - But you get "Access Denied" when visiting URL
# MAGIC
# MAGIC **Root Cause:**
# MAGIC - App permissions not configured
# MAGIC - User not in allowed group
# MAGIC
# MAGIC **Solution:**
# MAGIC ```
# MAGIC 1. Go to Databricks UI → Apps
# MAGIC 2. Find your app → Click "Permissions"
# MAGIC 3. Add users/groups who should have access
# MAGIC 4. Options:
# MAGIC    - Viewer: Can only view the app
# MAGIC    - Editor: Can modify the app code
# MAGIC    - Owner: Full control
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Still Having Issues?
# MAGIC
# MAGIC ### Debug Checklist
# MAGIC
# MAGIC Run through this checklist systematically:
# MAGIC
# MAGIC - [ ] **Completed Notebook 21**: Gold tables exist with data
# MAGIC - [ ] **Correct schema name**: Matches your personal schema
# MAGIC - [ ] **Libraries installed**: Streamlit and Plotly installed and Python restarted
# MAGIC - [ ] **Cluster running**: Databricks cluster is active
# MAGIC - [ ] **No code errors**: All cells run without exceptions
# MAGIC - [ ] **Data exists**: `SELECT COUNT(*)` returns rows
# MAGIC - [ ] **Columns match**: All required columns present in gold tables
# MAGIC
# MAGIC ### Getting Help
# MAGIC
# MAGIC If you're still stuck:
# MAGIC
# MAGIC 1. **Check error messages carefully**: Copy the full error text
# MAGIC 2. **Test data access directly**: Run SQL queries to verify tables
# MAGIC 3. **Start simple**: Comment out sections to isolate the problem
# MAGIC 4. **Ask for help**: Share:
# MAGIC    - Error message (full text)
# MAGIC    - What you've tried
# MAGIC    - Results of verification queries
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary
# MAGIC
# MAGIC ## What We Built
# MAGIC
# MAGIC ✅ **Interactive Stock Market Analyzer** with:
# MAGIC - Real-time data from Unity Catalog gold tables
# MAGIC - Multiple visualization types (bar, scatter, line, pie)
# MAGIC - Risk-return analysis
# MAGIC - Detailed stock analysis
# MAGIC - Portfolio simulator
# MAGIC - Responsive design with Streamlit
# MAGIC
# MAGIC ✅ **Production-Ready Features**:
# MAGIC - Data caching for performance
# MAGIC - Error handling and user feedback
# MAGIC - Configurable catalog and schema
# MAGIC - Clean, professional UI
# MAGIC - Help text and documentation
# MAGIC
# MAGIC ## Key Learnings
# MAGIC
# MAGIC 1. **Streamlit makes data apps simple**: Build complex UIs with pure Python
# MAGIC 2. **Unity Catalog integration is seamless**: Query tables directly from apps
# MAGIC 3. **Caching is critical**: Use `@st.cache_data` for expensive operations
# MAGIC 4. **User experience matters**: Loading indicators, error messages, help text
# MAGIC 5. **Deployment is flexible**: Notebook, Git, or SDK approaches
# MAGIC
# MAGIC ## Real-World Applications
# MAGIC
# MAGIC This pattern can be adapted for:
# MAGIC - **Sales dashboards**: Revenue, customer metrics, forecasts
# MAGIC - **Marketing analytics**: Campaign performance, attribution, ROI
# MAGIC - **Operations monitoring**: Inventory, logistics, quality metrics
# MAGIC - **ML model demos**: Interactive model exploration and prediction
# MAGIC - **Data quality tools**: Profiling, validation, monitoring
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Congratulations!** 🎉 You've completed the advanced Databricks Apps module and built a production-ready data application!