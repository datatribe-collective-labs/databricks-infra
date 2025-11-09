# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced: Stock Market Analyzer - Streamlit App
# MAGIC
# MAGIC ## About This App
# MAGIC
# MAGIC This Streamlit application demonstrates how to build an interactive data application on Databricks that:
# MAGIC - Queries Unity Catalog gold layer tables
# MAGIC - Provides interactive visualizations
# MAGIC - Enables self-service analytics
# MAGIC - Requires no coding knowledge for end users
# MAGIC
# MAGIC ## Data Source
# MAGIC
# MAGIC This app uses gold layer tables created in **Notebook 21 (Stock Market Wheel Deployment)**:
# MAGIC - `gold_stock_market_summary` - Aggregate performance metrics
# MAGIC - `gold_stock_market_detailed_analytics` - Daily time series with volatility
# MAGIC
# MAGIC ## Deployment Instructions
# MAGIC
# MAGIC ### Option 1: Deploy from This Notebook
# MAGIC 1. Run all cells to verify app works
# MAGIC 2. Click **Publish** → **Databricks App**
# MAGIC 3. Configure app name and settings
# MAGIC 4. Click **Deploy**
# MAGIC
# MAGIC ### Option 2: Deploy from Git Repository
# MAGIC 1. Copy the Streamlit code (cell below) to `app.py` in Git repo
# MAGIC 2. Add `requirements.txt` with dependencies
# MAGIC 3. Connect repo to Databricks Repos
# MAGIC 4. Deploy via Apps UI
# MAGIC
# MAGIC ---

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

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Stock Market Analyzer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Spark session
@st.cache_resource
def get_spark():
    """Get or create Spark session."""
    return SparkSession.builder.getOrCreate()

spark = get_spark()

# Data loading functions
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_stock_summary(catalog: str, schema: str):
    """Load stock market summary from gold layer."""
    table_name = f"{catalog}.{schema}.gold_stock_market_summary"
    try:
        df_spark = spark.table(table_name)
        df_pandas = df_spark.toPandas()
        return df_pandas
    except Exception as e:
        st.error(f"Failed to load summary data: {str(e)}")
        st.info(f"Attempted to load: {table_name}")
        st.info("Please ensure you've run Notebook 21 to create the gold tables.")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_stock_details(catalog: str, schema: str, symbol: str = None):
    """Load detailed stock data from gold layer."""
    table_name = f"{catalog}.{schema}.gold_stock_market_detailed_analytics"
    try:
        df_spark = spark.table(table_name)

        if symbol:
            df_spark = df_spark.filter(F.col("symbol") == symbol)

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

# Catalog and schema selection
catalog = st.sidebar.text_input("Unity Catalog", value="databricks_course", help="Enter your catalog name")
schema = st.sidebar.text_input("Schema", value="chanukya_pekala", help="Enter your schema name")

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