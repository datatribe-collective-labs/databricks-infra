# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced: Databricks Apps - Interactive Data Applications
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this guide, you will understand:
# MAGIC - What Databricks Apps are and when to use them
# MAGIC - How Databricks Apps differ from notebooks and dashboards
# MAGIC - The architecture and lifecycle of a Databricks App
# MAGIC - How to build Streamlit apps on Databricks
# MAGIC - Integration with Unity Catalog and gold layer data
# MAGIC - Deployment, sharing, and governance patterns
# MAGIC
# MAGIC ## What are Databricks Apps?
# MAGIC
# MAGIC **Databricks Apps** enable you to build and deploy interactive data applications directly on the Databricks platform:
# MAGIC
# MAGIC - **Framework Support**: Streamlit, Dash, Gradio, Flask, FastAPI
# MAGIC - **Native Integration**: Direct access to Unity Catalog tables
# MAGIC - **Serverless Deployment**: No cluster management required
# MAGIC - **Enterprise Features**: Authentication, access control, monitoring
# MAGIC - **Collaborative**: Share with stakeholders without Databricks accounts
# MAGIC
# MAGIC ## The Data Application Spectrum
# MAGIC
# MAGIC Understanding where Databricks Apps fit in the analytics ecosystem:
# MAGIC
# MAGIC ```
# MAGIC Notebooks                Dashboards              Databricks Apps
# MAGIC ├─────────────┼─────────────┼─────────────────┤
# MAGIC
# MAGIC NOTEBOOKS (Databricks Notebooks, Jupyter)
# MAGIC - Purpose: Exploration, development, documentation
# MAGIC - Audience: Data scientists, engineers
# MAGIC - Interactivity: Limited (widgets, parameters)
# MAGIC - Use cases: EDA, prototyping, documentation
# MAGIC - Example: Week 2-5 course notebooks
# MAGIC
# MAGIC DASHBOARDS (Lakeview, Tableau, Power BI)
# MAGIC - Purpose: Monitoring, reporting, visualization
# MAGIC - Audience: Business analysts, executives
# MAGIC - Interactivity: Medium (filters, drill-downs)
# MAGIC - Use cases: KPI tracking, reporting, metrics
# MAGIC - Example: Sales dashboard, marketing metrics
# MAGIC
# MAGIC DATABRICKS APPS (Streamlit, Dash, Custom)
# MAGIC - Purpose: Custom applications, interactive tools
# MAGIC - Audience: Any stakeholder (internal/external)
# MAGIC - Interactivity: High (full custom UX/UI)
# MAGIC - Use cases: ML demos, data tools, portals
# MAGIC - Example: Stock market analyzer (this module!)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: When to Use Databricks Apps
# MAGIC
# MAGIC ## Decision Framework
# MAGIC
# MAGIC | Requirement | Solution | Why? |
# MAGIC |------------|----------|------|
# MAGIC | "I need to explore data and document findings" | **Notebook** | Rich narrative + code + visuals |
# MAGIC | "I need to monitor KPIs and metrics daily" | **Dashboard** | Static visuals, scheduled refreshes |
# MAGIC | "I need users to interact with data dynamically" | **Databricks App** | Custom logic, rich interactivity |
# MAGIC | "I need to deploy an ML model as a service" | **Databricks App** | RESTful APIs, model serving |
# MAGIC | "I need to build a data entry form" | **Databricks App** | Custom forms, validation, writes |
# MAGIC | "I need to share insights with executives" | **Dashboard** | Professional visualizations |
# MAGIC | "I need a tool for citizen data scientists" | **Databricks App** | Guided workflows, no-code UX |
# MAGIC
# MAGIC ## Real-World Use Cases
# MAGIC
# MAGIC ### ✅ Great for Databricks Apps
# MAGIC
# MAGIC 1. **ML Model Demos**:
# MAGIC    - Sentiment analysis tool for marketing team
# MAGIC    - Customer churn prediction interface
# MAGIC    - Recommendation system explorer
# MAGIC
# MAGIC 2. **Self-Service Analytics**:
# MAGIC    - Stock market analyzer (our example!)
# MAGIC    - Sales forecasting tool
# MAGIC    - Customer segmentation explorer
# MAGIC
# MAGIC 3. **Data Quality Tools**:
# MAGIC    - Data profiling dashboard
# MAGIC    - Schema validation interface
# MAGIC    - Quality metrics explorer
# MAGIC
# MAGIC 4. **Operational Applications**:
# MAGIC    - Inventory management portal
# MAGIC    - Order tracking system
# MAGIC    - Resource allocation tool
# MAGIC
# MAGIC ### ❌ Not Ideal for Databricks Apps
# MAGIC
# MAGIC - **Static reporting**: Use Lakeview dashboards instead
# MAGIC - **Complex data pipelines**: Use Databricks Jobs/Workflows
# MAGIC - **Data exploration**: Use Databricks notebooks
# MAGIC - **High-frequency trading**: Use specialized platforms
# MAGIC - **Mobile-first apps**: Use dedicated mobile frameworks

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Databricks Apps Architecture
# MAGIC
# MAGIC ## How Databricks Apps Work
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │                        USER                               │
# MAGIC │              (Browser / API Client)                       │
# MAGIC └────────────────────┬─────────────────────────────────────┘
# MAGIC                      │ HTTPS
# MAGIC                      ▼
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │            DATABRICKS APP RUNTIME                         │
# MAGIC │  ┌────────────────────────────────────────────────────┐  │
# MAGIC │  │         Streamlit App (app.py)                     │  │
# MAGIC │  │  - UI Components (sliders, charts, tables)         │  │
# MAGIC │  │  - Business Logic (data filtering, calculations)   │  │
# MAGIC │  │  - Session State Management                        │  │
# MAGIC │  └─────────────────┬──────────────────────────────────┘  │
# MAGIC │                    │                                      │
# MAGIC │                    ▼                                      │
# MAGIC │  ┌────────────────────────────────────────────────────┐  │
# MAGIC │  │      Databricks SDK / Spark Session                │  │
# MAGIC │  │  - Query Unity Catalog tables                      │  │
# MAGIC │  │  - Execute Spark SQL                               │  │
# MAGIC │  │  - Access Delta tables                             │  │
# MAGIC │  └─────────────────┬──────────────────────────────────┘  │
# MAGIC └────────────────────┼──────────────────────────────────────┘
# MAGIC                      │
# MAGIC                      ▼
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │              UNITY CATALOG                                │
# MAGIC │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
# MAGIC │  │  Bronze      │  │  Silver      │  │  Gold        │   │
# MAGIC │  │  Tables      │→ │  Tables      │→ │  Tables      │   │
# MAGIC │  └──────────────┘  └──────────────┘  └──────────────┘   │
# MAGIC │                                                           │
# MAGIC │  Access Control:                                          │
# MAGIC │  - Table permissions (SELECT, MODIFY)                     │
# MAGIC │  - Column-level security                                  │
# MAGIC │  - Row-level filters                                      │
# MAGIC └───────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Key Components
# MAGIC
# MAGIC 1. **App Code** (`app.py` or notebook):
# MAGIC    - Streamlit/Dash/Gradio framework code
# MAGIC    - UI components and layout
# MAGIC    - Business logic and data processing
# MAGIC
# MAGIC 2. **Dependencies** (`requirements.txt`):
# MAGIC    - Python packages (streamlit, plotly, pandas)
# MAGIC    - Custom wheels (our stock_market_utils!)
# MAGIC    - External libraries
# MAGIC
# MAGIC 3. **Configuration** (`app.yaml`):
# MAGIC    - Runtime environment
# MAGIC    - Resource allocation
# MAGIC    - Access control
# MAGIC
# MAGIC 4. **Data Access**:
# MAGIC    - Unity Catalog integration
# MAGIC    - Spark session for queries
# MAGIC    - Caching for performance

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Building Apps with Streamlit
# MAGIC
# MAGIC ## Why Streamlit?
# MAGIC
# MAGIC Streamlit is the recommended framework for Databricks Apps because:
# MAGIC
# MAGIC ✅ **Pure Python**: No HTML/CSS/JavaScript required
# MAGIC ✅ **Rapid Development**: Build apps in minutes, not hours
# MAGIC ✅ **Rich Components**: Charts, tables, maps, forms out-of-the-box
# MAGIC ✅ **Reactive**: Automatic re-runs when inputs change
# MAGIC ✅ **Great Documentation**: Excellent tutorials and examples
# MAGIC ✅ **Large Community**: Active community and ecosystem
# MAGIC
# MAGIC ## Streamlit Core Concepts
# MAGIC
# MAGIC ### 1. Top-to-Bottom Execution
# MAGIC
# MAGIC Streamlit apps run **top to bottom** on every interaction:
# MAGIC
# MAGIC ```python
# MAGIC import streamlit as st
# MAGIC
# MAGIC st.title("My App")  # Runs first
# MAGIC
# MAGIC user_input = st.text_input("Enter name")  # Runs second
# MAGIC
# MAGIC st.write(f"Hello, {user_input}!")  # Runs third, re-runs when input changes
# MAGIC ```
# MAGIC
# MAGIC **Key Insight**: Every widget interaction triggers a full re-run from top to bottom.
# MAGIC
# MAGIC ### 2. Session State
# MAGIC
# MAGIC Persist data across re-runs using `st.session_state`:
# MAGIC
# MAGIC ```python
# MAGIC import streamlit as st
# MAGIC
# MAGIC # Initialize state
# MAGIC if "counter" not in st.session_state:
# MAGIC     st.session_state.counter = 0
# MAGIC
# MAGIC # Increment on button click
# MAGIC if st.button("Increment"):
# MAGIC     st.session_state.counter += 1
# MAGIC
# MAGIC st.write(f"Counter: {st.session_state.counter}")
# MAGIC ```
# MAGIC
# MAGIC ### 3. Caching
# MAGIC
# MAGIC Cache expensive operations with `@st.cache_data`:
# MAGIC
# MAGIC ```python
# MAGIC import streamlit as st
# MAGIC
# MAGIC @st.cache_data  # Results cached, won't re-run unless inputs change
# MAGIC def load_data(table_name):
# MAGIC     return spark.table(table_name).toPandas()
# MAGIC
# MAGIC df = load_data("databricks_course.user.gold_stock_summary")
# MAGIC st.dataframe(df)
# MAGIC ```
# MAGIC
# MAGIC **Cache Types**:
# MAGIC - `@st.cache_data`: For data (DataFrames, arrays, lists)
# MAGIC - `@st.cache_resource`: For resources (DB connections, ML models)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Essential Streamlit Components
# MAGIC
# MAGIC ### Layout Components
# MAGIC
# MAGIC ```python
# MAGIC import streamlit as st
# MAGIC
# MAGIC # Page configuration (must be first Streamlit command)
# MAGIC st.set_page_config(
# MAGIC     page_title="Stock Market Analyzer",
# MAGIC     page_icon="📈",
# MAGIC     layout="wide"  # or "centered"
# MAGIC )
# MAGIC
# MAGIC # Text elements
# MAGIC st.title("📈 Stock Market Analyzer")
# MAGIC st.header("Performance Dashboard")
# MAGIC st.subheader("Daily Returns")
# MAGIC st.text("Fixed-width text")
# MAGIC st.markdown("**Bold** and _italic_ text")
# MAGIC st.caption("Small caption text")
# MAGIC
# MAGIC # Columns
# MAGIC col1, col2, col3 = st.columns(3)
# MAGIC with col1:
# MAGIC     st.metric("AAPL", "$180.50", "+2.5%")
# MAGIC with col2:
# MAGIC     st.metric("GOOGL", "$142.30", "+1.2%")
# MAGIC with col3:
# MAGIC     st.metric("MSFT", "$380.10", "-0.8%")
# MAGIC
# MAGIC # Tabs
# MAGIC tab1, tab2, tab3 = st.tabs(["Overview", "Details", "Analysis"])
# MAGIC with tab1:
# MAGIC     st.write("Overview content")
# MAGIC with tab2:
# MAGIC     st.write("Details content")
# MAGIC
# MAGIC # Sidebar
# MAGIC st.sidebar.title("Filters")
# MAGIC stock_symbol = st.sidebar.selectbox("Select Stock", ["AAPL", "GOOGL", "MSFT"])
# MAGIC ```
# MAGIC
# MAGIC ### Input Widgets
# MAGIC
# MAGIC ```python
# MAGIC # Button
# MAGIC if st.button("Load Data"):
# MAGIC     st.write("Loading...")
# MAGIC
# MAGIC # Select box
# MAGIC symbol = st.selectbox("Choose stock", ["AAPL", "GOOGL", "MSFT"])
# MAGIC
# MAGIC # Multi-select
# MAGIC symbols = st.multiselect("Choose stocks", ["AAPL", "GOOGL", "MSFT", "AMZN"])
# MAGIC
# MAGIC # Slider
# MAGIC days = st.slider("Days to analyze", 1, 365, 30)
# MAGIC
# MAGIC # Date input
# MAGIC start_date = st.date_input("Start date")
# MAGIC
# MAGIC # Text input
# MAGIC user_name = st.text_input("Enter your name")
# MAGIC
# MAGIC # Number input
# MAGIC shares = st.number_input("Number of shares", min_value=1, max_value=1000, value=100)
# MAGIC
# MAGIC # Checkbox
# MAGIC show_details = st.checkbox("Show detailed analysis")
# MAGIC ```
# MAGIC
# MAGIC ### Display Components
# MAGIC
# MAGIC ```python
# MAGIC import pandas as pd
# MAGIC import plotly.express as px
# MAGIC
# MAGIC # DataFrame
# MAGIC df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
# MAGIC st.dataframe(df)  # Interactive table
# MAGIC st.table(df)      # Static table
# MAGIC
# MAGIC # Charts
# MAGIC st.line_chart(df)
# MAGIC st.bar_chart(df)
# MAGIC st.area_chart(df)
# MAGIC
# MAGIC # Plotly charts (recommended for custom visualizations)
# MAGIC fig = px.line(df, x="A", y="B", title="My Chart")
# MAGIC st.plotly_chart(fig, use_container_width=True)
# MAGIC
# MAGIC # Metrics
# MAGIC st.metric(
# MAGIC     label="Stock Price",
# MAGIC     value="$180.50",
# MAGIC     delta="+2.5%",
# MAGIC     delta_color="normal"  # or "inverse"
# MAGIC )
# MAGIC
# MAGIC # JSON
# MAGIC st.json({"key": "value", "nested": {"a": 1}})
# MAGIC
# MAGIC # Code
# MAGIC st.code('''
# MAGIC def hello():
# MAGIC     print("Hello, world!")
# MAGIC ''', language="python")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4: Databricks Integration Patterns
# MAGIC
# MAGIC ## Accessing Unity Catalog Tables
# MAGIC
# MAGIC ### Pattern 1: Direct Spark SQL
# MAGIC
# MAGIC ```python
# MAGIC import streamlit as st
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # Get Spark session (available in Databricks App runtime)
# MAGIC spark = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC @st.cache_data
# MAGIC def load_stock_summary():
# MAGIC     """Load gold layer summary table."""
# MAGIC     df_spark = spark.table("databricks_course.user_schema.gold_stock_market_summary")
# MAGIC     return df_spark.toPandas()  # Convert to pandas for Streamlit
# MAGIC
# MAGIC df = load_stock_summary()
# MAGIC st.dataframe(df)
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 2: SQL Queries with Parameters
# MAGIC
# MAGIC ```python
# MAGIC @st.cache_data
# MAGIC def load_stock_history(symbol: str, start_date: str, end_date: str):
# MAGIC     """Load historical data for a specific stock."""
# MAGIC     query = f"""
# MAGIC     SELECT date, close, daily_return, cumulative_return
# MAGIC     FROM databricks_course.user_schema.silver_stock_market_returns
# MAGIC     WHERE symbol = '{symbol}'
# MAGIC       AND date BETWEEN '{start_date}' AND '{end_date}'
# MAGIC     ORDER BY date
# MAGIC     """
# MAGIC     df_spark = spark.sql(query)
# MAGIC     return df_spark.toPandas()
# MAGIC
# MAGIC # In app
# MAGIC symbol = st.selectbox("Stock", ["AAPL", "GOOGL", "MSFT"])
# MAGIC df = load_stock_history(symbol, "2024-01-01", "2024-12-31")
# MAGIC st.line_chart(df.set_index("date")["close"])
# MAGIC ```
# MAGIC
# MAGIC ### Pattern 3: PySpark DataFrames
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC @st.cache_data
# MAGIC def calculate_portfolio_value(symbols: list, shares: dict):
# MAGIC     """Calculate portfolio value from multiple stocks."""
# MAGIC     df = spark.table("databricks_course.user_schema.gold_stock_market_summary")
# MAGIC
# MAGIC     # Filter to selected symbols
# MAGIC     df_filtered = df.filter(F.col("symbol").isin(symbols))
# MAGIC
# MAGIC     # Add shares and calculate value
# MAGIC     # (In real app, would use actual latest prices)
# MAGIC     for symbol, share_count in shares.items():
# MAGIC         df_filtered = df_filtered.withColumn(
# MAGIC             f"{symbol}_value",
# MAGIC             F.when(F.col("symbol") == symbol, F.col("avg_close") * share_count)
# MAGIC         )
# MAGIC
# MAGIC     return df_filtered.toPandas()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Security and Access Control
# MAGIC
# MAGIC ### Unity Catalog Permissions
# MAGIC
# MAGIC Databricks Apps respect Unity Catalog permissions:
# MAGIC
# MAGIC ```python
# MAGIC # User can only query tables they have SELECT permission on
# MAGIC df = spark.table("databricks_course.user_schema.gold_summary")  # ✅ Allowed
# MAGIC df = spark.table("restricted_catalog.sensitive.customer_pii")    # ❌ Denied
# MAGIC ```
# MAGIC
# MAGIC **Best Practices**:
# MAGIC - Grant minimal necessary permissions (SELECT, not MODIFY)
# MAGIC - Use row-level security for sensitive data
# MAGIC - Implement column masking for PII
# MAGIC - Audit app access in Unity Catalog audit logs
# MAGIC
# MAGIC ### App-Level Authentication
# MAGIC
# MAGIC Databricks Apps support multiple authentication modes:
# MAGIC
# MAGIC 1. **Databricks User Authentication** (default):
# MAGIC    - Users must have Databricks accounts
# MAGIC    - Inherits workspace permissions
# MAGIC    - Best for internal tools
# MAGIC
# MAGIC 2. **Public Access** (with caution):
# MAGIC    - No authentication required
# MAGIC    - App runs with service principal identity
# MAGIC    - Best for demos, public dashboards
# MAGIC    - **Warning**: Ensure no sensitive data exposed
# MAGIC
# MAGIC 3. **Custom Authentication**:
# MAGIC    - Implement OAuth, SAML, etc.
# MAGIC    - Manage users in app code
# MAGIC    - Best for external stakeholder access

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 5: Deployment and Lifecycle
# MAGIC
# MAGIC ## App Structure
# MAGIC
# MAGIC Recommended file organization for Databricks Apps:
# MAGIC
# MAGIC ```
# MAGIC stock-market-app/
# MAGIC ├── app.py                   # Main Streamlit application
# MAGIC ├── requirements.txt         # Python dependencies
# MAGIC ├── app.yaml                 # Databricks App configuration (optional)
# MAGIC ├── pages/                   # Multi-page apps
# MAGIC │   ├── 1_Overview.py
# MAGIC │   ├── 2_Analysis.py
# MAGIC │   └── 3_Portfolio.py
# MAGIC ├── utils/                   # Utility modules
# MAGIC │   ├── data_loader.py
# MAGIC │   ├── charts.py
# MAGIC │   └── calculations.py
# MAGIC └── README.md
# MAGIC ```
# MAGIC
# MAGIC ## Deployment Options
# MAGIC
# MAGIC ### Option 1: Deploy from Notebook (Quickest)
# MAGIC
# MAGIC 1. Create a Databricks notebook with Streamlit code
# MAGIC 2. Click **Publish** → **Databricks App**
# MAGIC 3. Configure name and settings
# MAGIC 4. Click **Deploy**
# MAGIC
# MAGIC **Pros**: Fastest for prototypes
# MAGIC **Cons**: Limited to single file, harder to version control
# MAGIC
# MAGIC ### Option 2: Deploy from Git Repository (Recommended)
# MAGIC
# MAGIC 1. Create Git repository with app code
# MAGIC 2. Connect repository to Databricks Repos
# MAGIC 3. In Databricks UI: **Apps** → **Create App**
# MAGIC 4. Select **Git repository** as source
# MAGIC 5. Configure branch and entry point (`app.py`)
# MAGIC 6. Deploy
# MAGIC
# MAGIC **Pros**: Version controlled, supports multi-file apps, CI/CD ready
# MAGIC **Cons**: Requires Git setup
# MAGIC
# MAGIC ### Option 3: Deploy via CLI/SDK (Automation)
# MAGIC
# MAGIC ```bash
# MAGIC # Using Databricks CLI
# MAGIC databricks apps create \
# MAGIC   --name "stock-market-app" \
# MAGIC   --source-code-path "/Workspace/Repos/username/stock-market-app" \
# MAGIC   --description "Stock market analytics dashboard"
# MAGIC
# MAGIC # Deploy/update
# MAGIC databricks apps deploy stock-market-app
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # Using Databricks SDK
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC app = w.apps.create(
# MAGIC     name="stock-market-app",
# MAGIC     source_code_path="/Workspace/Repos/username/stock-market-app",
# MAGIC     description="Stock market analytics dashboard"
# MAGIC )
# MAGIC
# MAGIC # Deploy
# MAGIC w.apps.deploy(app_name="stock-market-app")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Lifecycle Management
# MAGIC
# MAGIC ### Development Workflow
# MAGIC
# MAGIC ```
# MAGIC 1. DEVELOP
# MAGIC    ├── Write app code locally or in Databricks notebook
# MAGIC    ├── Test with sample data
# MAGIC    └── Iterate on UI/UX
# MAGIC
# MAGIC 2. TEST
# MAGIC    ├── Deploy to dev environment
# MAGIC    ├── Test with real Unity Catalog data
# MAGIC    ├── Validate permissions
# MAGIC    └── Performance testing
# MAGIC
# MAGIC 3. STAGE
# MAGIC    ├── Deploy to staging environment
# MAGIC    ├── User acceptance testing
# MAGIC    ├── Load testing
# MAGIC    └── Security review
# MAGIC
# MAGIC 4. PRODUCTION
# MAGIC    ├── Deploy to production
# MAGIC    ├── Monitor usage and performance
# MAGIC    ├── Collect user feedback
# MAGIC    └── Plan iterations
# MAGIC ```
# MAGIC
# MAGIC ### Versioning Strategy
# MAGIC
# MAGIC Use Git tags/branches for versions:
# MAGIC
# MAGIC - `main` branch → Production app
# MAGIC - `develop` branch → Development app
# MAGIC - `v1.0.0` tags → Release versions
# MAGIC
# MAGIC ### Monitoring
# MAGIC
# MAGIC Monitor your Databricks Apps:
# MAGIC
# MAGIC 1. **Usage Metrics**:
# MAGIC    - Number of users
# MAGIC    - Session duration
# MAGIC    - Popular features
# MAGIC
# MAGIC 2. **Performance Metrics**:
# MAGIC    - Load times
# MAGIC    - Query execution times
# MAGIC    - Cache hit rates
# MAGIC
# MAGIC 3. **Error Tracking**:
# MAGIC    - Application errors
# MAGIC    - Failed queries
# MAGIC    - Permission denials
# MAGIC
# MAGIC 4. **Cost Tracking**:
# MAGIC    - Compute usage
# MAGIC    - Data access patterns
# MAGIC    - Optimization opportunities

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 6: Best Practices
# MAGIC
# MAGIC ## Performance Optimization
# MAGIC
# MAGIC ### 1. Cache Aggressively
# MAGIC
# MAGIC ```python
# MAGIC @st.cache_data(ttl=3600)  # Cache for 1 hour
# MAGIC def load_data():
# MAGIC     return spark.table("my_table").toPandas()
# MAGIC ```
# MAGIC
# MAGIC ### 2. Limit Data Volume
# MAGIC
# MAGIC ```python
# MAGIC # ❌ BAD: Load millions of rows
# MAGIC df = spark.table("huge_table").toPandas()
# MAGIC
# MAGIC # ✅ GOOD: Filter and aggregate first
# MAGIC df = spark.sql("""
# MAGIC     SELECT symbol, AVG(close) as avg_close
# MAGIC     FROM huge_table
# MAGIC     WHERE date >= '2024-01-01'
# MAGIC     GROUP BY symbol
# MAGIC """).toPandas()
# MAGIC ```
# MAGIC
# MAGIC ### 3. Use Efficient Visualizations
# MAGIC
# MAGIC ```python
# MAGIC # ❌ SLOW: Large scatter plot with millions of points
# MAGIC st.plotly_chart(px.scatter(huge_df, x="x", y="y"))
# MAGIC
# MAGIC # ✅ FAST: Aggregate or sample first
# MAGIC sampled_df = huge_df.sample(n=10000)
# MAGIC st.plotly_chart(px.scatter(sampled_df, x="x", y="y"))
# MAGIC ```
# MAGIC
# MAGIC ### 4. Optimize Re-runs
# MAGIC
# MAGIC ```python
# MAGIC # Use forms to batch inputs
# MAGIC with st.form("filters"):
# MAGIC     symbol = st.selectbox("Stock", ["AAPL", "GOOGL"])
# MAGIC     start_date = st.date_input("Start")
# MAGIC     end_date = st.date_input("End")
# MAGIC     submitted = st.form_submit_button("Apply Filters")
# MAGIC
# MAGIC # Only runs when "Apply Filters" is clicked
# MAGIC if submitted:
# MAGIC     df = load_data(symbol, start_date, end_date)
# MAGIC     st.dataframe(df)
# MAGIC ```
# MAGIC
# MAGIC ## Security Best Practices
# MAGIC
# MAGIC ### 1. Never Hardcode Credentials
# MAGIC
# MAGIC ```python
# MAGIC # ❌ NEVER DO THIS
# MAGIC api_key = "sk-1234567890abcdef"
# MAGIC
# MAGIC # ✅ Use Databricks Secrets
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC api_key = w.secrets.get_secret(scope="my-scope", key="api-key")
# MAGIC ```
# MAGIC
# MAGIC ### 2. Validate User Inputs
# MAGIC
# MAGIC ```python
# MAGIC # Prevent SQL injection
# MAGIC symbol = st.text_input("Stock symbol")
# MAGIC
# MAGIC # ❌ VULNERABLE
# MAGIC query = f"SELECT * FROM stocks WHERE symbol = '{symbol}'"
# MAGIC
# MAGIC # ✅ SAFE: Use parameterized queries or validation
# MAGIC if symbol.isalpha() and len(symbol) <= 5:
# MAGIC     df = spark.table("stocks").filter(F.col("symbol") == symbol)
# MAGIC else:
# MAGIC     st.error("Invalid symbol")
# MAGIC ```
# MAGIC
# MAGIC ### 3. Implement Access Controls
# MAGIC
# MAGIC ```python
# MAGIC # Check user permissions before showing data
# MAGIC current_user = w.current_user.me().user_name
# MAGIC
# MAGIC if current_user in ADMIN_USERS:
# MAGIC     st.write("Admin dashboard")
# MAGIC     show_admin_features()
# MAGIC else:
# MAGIC     st.write("User dashboard")
# MAGIC     show_user_features()
# MAGIC ```
# MAGIC
# MAGIC ## UX Best Practices
# MAGIC
# MAGIC 1. **Provide Loading Feedback**:
# MAGIC    ```python
# MAGIC    with st.spinner("Loading data..."):
# MAGIC        df = load_data()
# MAGIC    st.success("Data loaded!")
# MAGIC    ```
# MAGIC
# MAGIC 2. **Handle Errors Gracefully**:
# MAGIC    ```python
# MAGIC    try:
# MAGIC        df = load_data()
# MAGIC        st.dataframe(df)
# MAGIC    except Exception as e:
# MAGIC        st.error(f"Failed to load data: {str(e)}")
# MAGIC        st.info("Please try again or contact support.")
# MAGIC    ```
# MAGIC
# MAGIC 3. **Use Clear Labels and Help Text**:
# MAGIC    ```python
# MAGIC    st.selectbox(
# MAGIC        "Stock Symbol",
# MAGIC        ["AAPL", "GOOGL"],
# MAGIC        help="Select a stock to analyze"
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 4. **Provide Context**:
# MAGIC    ```python
# MAGIC    st.metric(
# MAGIC        "Stock Price",
# MAGIC        "$180.50",
# MAGIC        "+2.5%",
# MAGIC        help="Closing price as of market close"
# MAGIC    )
# MAGIC    ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Databricks Apps fill the gap** between notebooks (for developers) and dashboards (for stakeholders)
# MAGIC 2. **Streamlit is the fastest path** to building interactive data applications
# MAGIC 3. **Unity Catalog integration** provides secure, governed data access
# MAGIC 4. **Caching is critical** for performance in data-intensive apps
# MAGIC 5. **Git-based deployment** enables version control and CI/CD
# MAGIC 6. **Security is paramount** - validate inputs, use secrets, respect permissions
# MAGIC
# MAGIC ## When to Use Databricks Apps
# MAGIC
# MAGIC ✅ **Use Databricks Apps when you need**:
# MAGIC - Custom interactive experiences
# MAGIC - Self-service analytics tools
# MAGIC - ML model demos and explorers
# MAGIC - Operational dashboards with write-back
# MAGIC - Data quality and monitoring tools
# MAGIC
# MAGIC ❌ **Don't use Databricks Apps when**:
# MAGIC - Static reporting suffices (use Lakeview)
# MAGIC - Complex ETL needed (use Jobs/Workflows)
# MAGIC - Pure exploration (use notebooks)
# MAGIC - Mobile-first required (use dedicated frameworks)
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next notebook, we'll build a **Stock Market Analyzer** app that:
# MAGIC - Visualizes gold layer data from our Week 5 pipeline
# MAGIC - Provides interactive stock performance analysis
# MAGIC - Demonstrates real-world Streamlit + Databricks patterns
# MAGIC - Shows deployment and sharing workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Resources**:
# MAGIC - [Databricks Apps Documentation](https://docs.databricks.com/applications/index.html)
# MAGIC - [Streamlit Documentation](https://docs.streamlit.io/)
# MAGIC - [Streamlit Gallery](https://streamlit.io/gallery)
# MAGIC - [Databricks Apps Examples](https://github.com/databricks/apps-examples)