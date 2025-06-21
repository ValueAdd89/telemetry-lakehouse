import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go 
from pathlib import Path
import numpy as np 

# --- Page Configuration ---
st.set_page_config(
    layout="wide",
    page_title="Telemetry Lakehouse Dashboard",
    page_icon="📊"
)

st.title("📊 Telemetry Lakehouse Dashboard")
st.markdown("Use this dashboard to explore product feature usage, user behavior, and engagement over time.")
st.markdown("---")

# --- Data Loading (Cached for performance) ---
# streamlit_app/dashboard.py - Updated load_data function
@st.cache_data  
def load_data():
    """Load data from dbt outputs (Parquet format)"""
    dbt_target_path = Path(__file__).parent.parent / "dbt" / "target" / "run" / "telemetry_lakehouse" / "models" / "marts"
    
    try:
        # Read Parquet files instead of CSV (better performance)
        df_feature_events = pd.read_parquet(dbt_target_path / "mart_feature_usage_hourly.parquet")
        df_users = pd.read_parquet(dbt_target_path / "mart_user_profiles.parquet")
        df_user_sessions = pd.read_parquet(dbt_target_path / "mart_user_sessions.parquet")
        df_top_features = pd.read_parquet(dbt_target_path / "mart_top_features.parquet")
        
        # Parse dates
        df_feature_events['window_start'] = pd.to_datetime(df_feature_events['window_start'])
        
        return df_feature_events, df_users, df_user_sessions, df_top_features
        
    except FileNotFoundError:
        # Fallback: Try to read from database if available
        return load_from_database()
    except:
        st.error("dbt models not found. Run 'cd dbt && dbt run' first.")
        st.stop()

# === Alternative: Database Connection (for Production) ===
@st.cache_data
def load_data_from_warehouse():
    """
    Alternative: Load data directly from data warehouse
    (For production deployment where dbt writes to warehouse)
    """
    import sqlalchemy
    
    # Connect to data warehouse (Snowflake, BigQuery, etc.)
    engine = sqlalchemy.create_engine('postgresql://user:pass@localhost/telemetry_db')
    
    try:
        # Load mart tables from warehouse
        df_feature_events = pd.read_sql("""
            SELECT * FROM marts.mart_feature_usage_hourly 
            WHERE window_start >= CURRENT_DATE - INTERVAL '90 days'
        """, engine, parse_dates=['window_start'])
        
        df_users = pd.read_sql("SELECT * FROM marts.mart_user_profiles", engine)
        
        df_user_sessions = pd.read_sql("""
            SELECT * FROM marts.mart_user_sessions
            WHERE session_start >= CURRENT_DATE - INTERVAL '90 days'  
        """, engine, parse_dates=['session_start', 'session_end'])
        
        # ... load other marts
        
        return df_feature_events, df_users, df_user_sessions
        
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.info("Falling back to CSV-based mart loading...")
        return load_data()  # Fallback to CSV approach

# === Load Data Using Proper Architecture ===
# Use CSV-based marts for development, database marts for production
USE_WAREHOUSE = False  # Set to True for production database deployment

if USE_WAREHOUSE:
    (df_all_data, df_users, df_user_sessions, df_top_features, 
     df_funnel_analysis, df_overview_kpis, df_time_aggregations) = load_data_from_warehouse()
else:
    (df_all_data, df_users, df_user_sessions, df_top_features, 
     df_funnel_analysis, df_overview_kpis, df_time_aggregations) = load_data()


# --- Sidebar Filters (Updated for Mart Data) ---
st.sidebar.header("Filter Data")

# Date Range Filter (using mart data date range)
if not df_all_data['window_start'].empty:
    min_date_val = df_all_data['window_start'].min().date()
    max_date_val = df_all_data['window_start'].max().date()
    
    st.sidebar.info(f"📅 Data available: {min_date_val} to {max_date_val}")
else:
    min_date_val = pd.to_datetime('2024-01-01').date() 
    max_date_val = pd.to_datetime('2024-01-31').date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date_val, max_date_val),
    min_value=min_date_val,
    max_value=max_date_val
)

# Time Granularity Selector
time_granularity = st.sidebar.selectbox(
    "Select Time Granularity",
    options=["Daily", "Weekly", "Monthly"]
)

# Feature Selection (using mart data)
all_features_options = ['All'] + sorted(df_all_data['feature'].unique().tolist())
selected_feature = st.sidebar.selectbox("Select Feature", options=all_features_options, index=0)

# User Selection (using mart data)
all_users_options = ['All'] + sorted(df_all_data['user_id'].unique().tolist())
selected_user = st.sidebar.selectbox("Select User", options=all_users_options, index=0)

# Top N Features Slider
top_n_features = st.sidebar.slider("Show Top N Features", min_value=5, max_value=20, value=10)

# Data lineage info
with st.sidebar.expander("📊 Data Lineage"):
    st.markdown("""
    **Data Flow:**
    1. 📁 Raw CSV files
    2. 🔄 dbt Staging models (cleaning)
    3. ⚙️ dbt Intermediate models (business logic)
    4. 📊 dbt Mart models (analytics-ready)
    5. 📈 Streamlit Dashboard (this app)
    
    **Current Source:** dbt Mart Models
    """)

# === Apply Filters to Mart Data ===
if len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) 
else: 
    start_date = pd.to_datetime(min_date_val)
    end_date = pd.to_datetime(max_date_val) + pd.Timedelta(days=1)

# Filter mart data
df_filtered = df_all_data[
    (df_all_data['window_start'] >= start_date) &
    (df_all_data['window_start'] < end_date) 
].copy() 

# Apply feature filter
if selected_feature != 'All':
    df_filtered = df_filtered[df_filtered['feature'] == selected_feature].copy()

# Apply user filter  
if selected_user != 'All':
    df_filtered = df_filtered[df_filtered['user_id'] == selected_user].copy()

# Filter session data
df_sessions_filtered = df_user_sessions[
    (df_user_sessions['session_start'] >= start_date) &
    (df_user_sessions['session_start'] < end_date)
].copy()

if selected_user != 'All':
    df_sessions_filtered = df_sessions_filtered[
        df_sessions_filtered['user_id'] == selected_user
    ].copy()

# Check if filtered data is empty
if df_filtered.empty:
    st.warning("No data available for the selected filters. Please adjust your selections.")
    st.stop()

# === Dashboard Tabs (Using Mart Data) ===
tab_names = ["📈 Overview", "🔍 Feature Analysis", "👥 User Insights", "🏆 Top Features", "⏱ Session Analysis", "📉 Funnel Analysis"]
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_names)

# --- Tab 1: Overview (Using Pre-calculated KPIs) ---
with tab1:
    st.header("Product Usage Summary")

    # Use pre-calculated KPIs from mart_dashboard_overview
    if not df_overview_kpis.empty:
        kpi_row = df_overview_kpis.iloc[0]  # Single row with aggregated KPIs
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", f"{int(kpi_row['total_events']):,}")
        col2.metric("Unique Users", f"{int(kpi_row['unique_users']):,}")
        col3.metric("Avg Events / User", f"{kpi_row['avg_events_per_user']:.1f}")
        col4.metric("Avg Session Duration", f"{kpi_row['avg_session_duration_hours']:.1f}h")
    else:
        # Fallback to filtered data calculations
        total_events_kpi = df_filtered['event_count'].sum()
        unique_users_kpi = df_filtered['user_id'].nunique()
        avg_events_per_user = total_events_kpi / unique_users_kpi if unique_users_kpi > 0 else 0
        avg_session_duration = df_sessions_filtered['session_duration_hours'].mean()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", f"{int(total_events_kpi):,}")
        col2.metric("Unique Users", f"{unique_users_kpi:,}")
        col3.metric("Avg Events / User", f"{avg_events_per_user:.1f}")
        col4.metric("Avg Session Duration", f"{avg_session_duration:.1f}h")

    st.markdown("---")

    # Use pre-aggregated time data from mart_feature_usage_by_time
    st.subheader("Total Events Over Time")
    
    if not df_time_aggregations.empty:
        # Use pre-calculated time aggregations instead of on-the-fly groupby
        time_col_map = {
            "Daily": "day_start",
            "Weekly": "week_start", 
            "Monthly": "month_start"
        }
        
        time_col = time_col_map[time_granularity]
        
        # Filter time aggregations by date range and user/feature
        time_data_filtered = df_time_aggregations[
            (df_time_aggregations[time_col] >= start_date.normalize()) &
            (df_time_aggregations[time_col] < end_date.normalize())
        ].copy()
        
        if selected_feature != 'All':
            time_data_filtered = time_data_filtered[time_data_filtered['feature'] == selected_feature]
        if selected_user != 'All':
            time_data_filtered = time_data_filtered[time_data_filtered['user_id'] == selected_user]
            
        if not time_data_filtered.empty:
            events_over_time = time_data_filtered.groupby(time_col)['event_count'].sum().reset_index()
            events_over_time.columns = ['Date', 'Total Events']
            
            fig_events_time = px.line(events_over_time, x="Date", y="Total Events", 
                                    title=f"Total Events ({time_granularity}) - From dbt Marts")
            st.plotly_chart(fig_events_time, use_container_width=True)
        else:
            st.info("No time-aggregated data available for current filters.")
    else:
        st.info("Pre-aggregated time data not available. Please run dbt models.")

# Continue with other tabs using mart data...
# (The rest of the tabs would follow similar pattern - using mart data instead of raw calculations)

st.sidebar.markdown("---")
st.sidebar.markdown("**🎯 Powered by dbt Marts**")
st.sidebar.caption("Analytics-ready data from the lakehouse")
