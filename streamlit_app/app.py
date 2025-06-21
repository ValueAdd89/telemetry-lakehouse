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

# --- Data Loading Functions ---
@st.cache_data
def load_data_from_csv():
    """
    Fallback: Load data from original CSV files
    """
    # Fixed path to point to spark_processed directory
    base_path = Path(__file__).parent.parent / "data" / "spark_processed"

    try:
        # Load original CSV files as fallback
        df_feature_events = pd.read_csv(base_path / "feature_usage_hourly_sample.csv", parse_dates=["window_start"])
        df_users = pd.read_csv(base_path / "users.csv")
        df_funnel_onboarding = pd.read_csv(base_path / "funnel_onboarding.csv", parse_dates=["timestamp"])
        df_funnel_feature_adoption = pd.read_csv(base_path / "funnel_feature_adoption.csv", parse_dates=["timestamp"])
        df_funnel_workflow_completion = pd.read_csv(base_path / "funnel_workflow_completion.csv", parse_dates=["timestamp"])

        # Data cleaning
        df_feature_events['window_start'] = pd.to_datetime(df_feature_events['window_start'], errors='coerce')
        df_feature_events.dropna(subset=['window_start'], inplace=True)

        # Merge with users
        df_all_data_merged = pd.merge(df_feature_events, df_users, on='user_id', how='left')

        # Create session data on-the-fly (as original dashboard did)
        session_funnel = (
            df_all_data_merged.groupby("user_id") 
            .agg(session_start=("window_start", "min"),
                 session_end=("window_start", "max"),
                 feature_count=("feature", "nunique"),
                 total_events=("event_count", "sum")) 
            .reset_index()
        )
        session_funnel['session_duration_hours'] = (session_funnel['session_end'] - session_funnel['session_start']).dt.total_seconds() / 3600

        # Create dummy mart-like data for compatibility
        top_features = df_feature_events.groupby('feature')['event_count'].sum().reset_index()
        top_features = top_features.sort_values('event_count', ascending=False)
        top_features['rank'] = range(1, len(top_features) + 1)
        
        funnel_analysis = pd.DataFrame()  # Empty for now
        overview_kpis = pd.DataFrame({
            'total_events': [df_feature_events['event_count'].sum()],
            'unique_users': [df_feature_events['user_id'].nunique()],
            'avg_events_per_user': [df_feature_events['event_count'].sum() / df_feature_events['user_id'].nunique()],
            'avg_session_duration_hours': [session_funnel['session_duration_hours'].mean()]
        })
        
        time_aggregations = df_feature_events.copy()
        time_aggregations['day_start'] = time_aggregations['window_start'].dt.floor('D')
        time_aggregations['week_start'] = time_aggregations['window_start'].dt.to_period('W').dt.start_time
        time_aggregations['month_start'] = time_aggregations['window_start'].dt.to_period('M').dt.start_time

        st.success(f"✅ Loaded {len(df_feature_events):,} feature events from CSV files")
        st.info("📊 Data sourced from: Spark processed CSV files (dbt marts not available)")
        
        return (
            df_all_data_merged,
            df_users, 
            session_funnel, 
            top_features,
            funnel_analysis,
            overview_kpis,
            time_aggregations
        )
        
    except Exception as e:
        st.error(f"Error loading CSV files from {base_path}: {e}")
        
        # Additional debugging info
        st.error("**Debug Info:**")
        st.write(f"Looking for files in: {base_path}")
        if base_path.exists():
            st.write("Directory exists. Files found:")
            for file in base_path.glob("*.csv"):
                st.write(f"  - {file.name}")
        else:
            st.write("Directory does not exist!")
            
        # Try alternative paths
        alt_paths = [
            Path(__file__).parent / "data" / "spark_processed",
            Path("data") / "spark_processed",
            Path("../data/spark_processed"),
        ]
        
        st.write("\n**Trying alternative paths:**")
        for alt_path in alt_paths:
            if alt_path.exists():
                st.write(f"✅ Found: {alt_path}")
                csv_files = list(alt_path.glob("*.csv"))
                if csv_files:
                    st.write("  CSV files:")
                    for file in csv_files[:5]:  # Show first 5
                        st.write(f"    - {file.name}")
                break
            else:
                st.write(f"❌ Not found: {alt_path}")
        
        st.stop()

@st.cache_data
def load_data_from_dbt_outputs():
    """
    Primary: Load data from dbt target outputs
    """
    # Try different possible dbt output locations
    possible_paths = [
        Path(__file__).parent.parent / "dbt" / "target",
        Path(__file__).parent.parent / "dbt" / "target" / "run" / "telemetry_lakehouse" / "models" / "marts",
        Path(__file__).parent.parent / "target" / "marts",
    ]
    
    for dbt_path in possible_paths:
        try:
            st.info(f"🔍 Checking for dbt outputs in: {dbt_path}")
            
            # Look for different file formats
            formats_to_try = ['.csv', '.parquet', '.json']
            
            for fmt in formats_to_try:
                feature_file = dbt_path / f"mart_feature_usage_hourly{fmt}"
                if feature_file.exists():
                    st.success(f"✅ Found dbt outputs in {fmt} format at: {dbt_path}")
                    
                    # Load based on format
                    if fmt == '.csv':
                        df_feature_events = pd.read_csv(feature_file, parse_dates=["window_start"])
                        df_users = pd.read_csv(dbt_path / f"mart_user_profiles{fmt}")
                        df_user_sessions = pd.read_csv(dbt_path / f"mart_user_sessions{fmt}", parse_dates=["session_start", "session_end"])
                        df_top_features = pd.read_csv(dbt_path / f"mart_top_features{fmt}")
                        df_overview_kpis = pd.read_csv(dbt_path / f"mart_dashboard_overview{fmt}")
                        df_time_aggregations = pd.read_csv(dbt_path / f"mart_feature_usage_by_time{fmt}")
                        df_funnel_analysis = pd.read_csv(dbt_path / f"mart_funnel_analysis{fmt}") if (dbt_path / f"mart_funnel_analysis{fmt}").exists() else pd.DataFrame()
                        
                    elif fmt == '.parquet':
                        df_feature_events = pd.read_parquet(feature_file)
                        df_users = pd.read_parquet(dbt_path / f"mart_user_profiles{fmt}")
                        df_user_sessions = pd.read_parquet(dbt_path / f"mart_user_sessions{fmt}")
                        df_top_features = pd.read_parquet(dbt_path / f"mart_top_features{fmt}")
                        df_overview_kpis = pd.read_parquet(dbt_path / f"mart_dashboard_overview{fmt}")
                        df_time_aggregations = pd.read_parquet(dbt_path / f"mart_feature_usage_by_time{fmt}")
                        df_funnel_analysis = pd.read_parquet(dbt_path / f"mart_funnel_analysis{fmt}") if (dbt_path / f"mart_funnel_analysis{fmt}").exists() else pd.DataFrame()
                    
                    # Merge feature events with users
                    df_all_data_merged = pd.merge(df_feature_events, df_users, on='user_id', how='left')
                    
                    st.success(f"✅ Loaded {len(df_feature_events):,} feature events from dbt marts")
                    st.info("📊 Data sourced from: CSV → dbt staging → dbt intermediate → dbt marts")
                    
                    return (
                        df_all_data_merged,
                        df_users, 
                        df_user_sessions, 
                        df_top_features,
                        df_funnel_analysis,
                        df_overview_kpis,
                        df_time_aggregations
                    )
        except Exception as e:
            st.warning(f"Could not load from {dbt_path}: {e}")
            continue
    
    return None

@st.cache_data
def load_data():
    """
    Main data loading function with fallback strategy
    """
    # Try to load from dbt outputs first
    dbt_data = load_data_from_dbt_outputs()
    if dbt_data is not None:
        return dbt_data
    
    # Fallback to CSV files
    st.warning("🔄 dbt outputs not found. Falling back to Spark processed CSV files.")
    st.info("""
    **To use dbt marts:**
    1. Navigate to dbt directory: `cd dbt`
    2. Run dbt models: `dbt run`
    3. Refresh this dashboard
    """)
    
    return load_data_from_csv()

# === Load Data Using Fallback Strategy ===
try:
    (df_all_data, df_users, df_user_sessions, df_top_features, 
     df_funnel_analysis, df_overview_kpis, df_time_aggregations) = load_data()
except Exception as e:
    st.error(f"""
    🚨 **Critical Error Loading Data**
    
    Error: {str(e)}
    
    **Troubleshooting Steps:**
    1. Check that data files exist in `data/spark_processed/` folder
    2. If using dbt: Run `cd dbt && dbt run` 
    3. Ensure CSV files have correct column names
    4. Refresh the page
    """)
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filter Data")

# Date Range Filter
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

# Feature Selection
all_features_options = ['All'] + sorted(df_all_data['feature'].unique().tolist())
selected_feature = st.sidebar.selectbox("Select Feature", options=all_features_options, index=0)

# User Selection
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
    
    **Current Source:** Auto-detected
    """)

# === Apply Filters ===
if len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) 
else: 
    start_date = pd.to_datetime(min_date_val)
    end_date = pd.to_datetime(max_date_val) + pd.Timedelta(days=1)

# Filter data
df_filtered = df_all_data[
    (df_all_data['window_start'] >= start_date) &
    (df_all_data['window_start'] < end_date) 
].copy() 

if selected_feature != 'All':
    df_filtered = df_filtered[df_filtered['feature'] == selected_feature].copy()

if selected_user != 'All':
    df_filtered = df_filtered[df_filtered['user_id'] == selected_user].copy()

# Filter session data
df_sessions_filtered = df_user_sessions[
    (df_user_sessions['session_start'] >= start_date) &
    (df_user_sessions['session_start'] < end_date)
].copy() if 'session_start' in df_user_sessions.columns else df_user_sessions.copy()

if selected_user != 'All' and 'user_id' in df_sessions_filtered.columns:
    df_sessions_filtered = df_sessions_filtered[
        df_sessions_filtered['user_id'] == selected_user
    ].copy()

# Check if filtered data is empty
if df_filtered.empty:
    st.warning("No data available for the selected filters. Please adjust your selections.")
    st.stop()

# === Dashboard Tabs ===
tab_names = ["📈 Overview", "🔍 Feature Analysis", "👥 User Insights", "🏆 Top Features", "⏱ Session Analysis", "📉 Funnel Analysis"]
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_names)

# --- Tab 1: Overview ---
with tab1:
    st.header("Product Usage Summary")

    # Use pre-calculated KPIs if available, otherwise calculate
    if not df_overview_kpis.empty and 'total_events' in df_overview_kpis.columns:
        kpi_row = df_overview_kpis.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", f"{int(kpi_row['total_events']):,}")
        col2.metric("Unique Users", f"{int(kpi_row['unique_users']):,}")
        col3.metric("Avg Events / User", f"{kpi_row['avg_events_per_user']:.1f}")
        col4.metric("Avg Session Duration", f"{kpi_row['avg_session_duration_hours']:.1f}h")
    else:
        # Fallback calculations
        total_events_kpi = df_filtered['event_count'].sum()
        unique_users_kpi = df_filtered['user_id'].nunique()
        avg_events_per_user = total_events_kpi / unique_users_kpi if unique_users_kpi > 0 else 0
        avg_session_duration = df_sessions_filtered['session_duration_hours'].mean() if 'session_duration_hours' in df_sessions_filtered.columns else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", f"{int(total_events_kpi):,}")
        col2.metric("Unique Users", f"{unique_users_kpi:,}")
        col3.metric("Avg Events / User", f"{avg_events_per_user:.1f}")
        col4.metric("Avg Session Duration", f"{avg_session_duration:.1f}h")

    st.markdown("---")

    # Time trends
    st.subheader("Total Events Over Time")
    
    # Create time groupings
    if time_granularity == "Daily":
        freq = 'D'
    elif time_granularity == "Weekly":
        freq = 'W'
    else:
        freq = 'MS'

    df_filtered['time_group'] = df_filtered['window_start'].dt.to_period(freq).dt.to_timestamp()
    events_over_time = df_filtered.groupby('time_group')['event_count'].sum().reset_index()
    events_over_time.columns = ['Date', 'Total Events']

    if not events_over_time.empty:
        fig_events_time = px.line(events_over_time, x="Date", y="Total Events", 
                                title=f"Total Events ({time_granularity})")
        st.plotly_chart(fig_events_time, use_container_width=True)
    else:
        st.info("No event data over time for the current selection.")

# --- Tab 2: Feature Analysis ---
with tab2:
    st.header("Feature Usage Analysis")
    
    # Feature usage over time
    st.subheader("Feature Usage Trends")
    
    df_filtered['time_group'] = df_filtered['window_start'].dt.to_period(freq).dt.to_timestamp()
    feature_time = df_filtered.groupby(['time_group', 'feature'])['event_count'].sum().reset_index()
    
    if not feature_time.empty:
        # Show top features only for readability
        top_features_list = df_filtered.groupby('feature')['event_count'].sum().nlargest(top_n_features).index.tolist()
        feature_time_filtered = feature_time[feature_time['feature'].isin(top_features_list)]
        
        fig_feature_trends = px.line(feature_time_filtered, x="time_group", y="event_count", 
                                   color="feature", title=f"Top {top_n_features} Feature Usage Over Time")
        st.plotly_chart(fig_feature_trends, use_container_width=True)
    
    # Feature distribution
    st.subheader("Feature Usage Distribution")
    feature_summary = df_filtered.groupby('feature')['event_count'].sum().reset_index()
    feature_summary = feature_summary.sort_values('event_count', ascending=False).head(top_n_features)
    
    if not feature_summary.empty:
        fig_feature_bar = px.bar(feature_summary, x="feature", y="event_count", 
                               title=f"Top {top_n_features} Features by Usage")
        fig_feature_bar.update_xaxes(tickangle=45)
        st.plotly_chart(fig_feature_bar, use_container_width=True)

# --- Tab 3: User Insights ---
with tab3:
    st.header("User Behavior Analysis")
    
    # User activity distribution
    st.subheader("User Activity Distribution")
    user_activity = df_filtered.groupby('user_id')['event_count'].sum().reset_index()
    user_activity.columns = ['user_id', 'total_events']
    
    if not user_activity.empty:
        fig_user_dist = px.histogram(user_activity, x="total_events", nbins=20,
                                   title="Distribution of User Activity Levels")
        st.plotly_chart(fig_user_dist, use_container_width=True)
    
    # Top users
    st.subheader("Most Active Users")
    top_users = user_activity.nlargest(10, 'total_events')
    
    if not top_users.empty:
        fig_top_users = px.bar(top_users, x="user_id", y="total_events",
                             title="Top 10 Most Active Users")
        st.plotly_chart(fig_top_users, use_container_width=True)

# --- Tab 4: Top Features ---
with tab4:
    st.header("Feature Rankings")
    
    if not df_top_features.empty and 'rank' in df_top_features.columns:
        # Use pre-calculated rankings
        top_features_display = df_top_features.head(top_n_features)
        st.dataframe(top_features_display, use_container_width=True)
    else:
        # Calculate rankings
        feature_rankings = df_filtered.groupby('feature').agg({
            'event_count': 'sum',
            'user_id': 'nunique'
        }).reset_index()
        feature_rankings.columns = ['feature', 'total_events', 'unique_users']
        feature_rankings = feature_rankings.sort_values('total_events', ascending=False)
        feature_rankings['rank'] = range(1, len(feature_rankings) + 1)
        
        st.dataframe(feature_rankings.head(top_n_features), use_container_width=True)

# --- Tab 5: Session Analysis ---
with tab5:
    st.header("Session Analytics")
    
    if 'session_duration_hours' in df_sessions_filtered.columns and not df_sessions_filtered.empty:
        # Session duration distribution
        st.subheader("Session Duration Distribution")
        fig_session_dist = px.histogram(df_sessions_filtered, x="session_duration_hours", nbins=20,
                                      title="Distribution of Session Durations")
        st.plotly_chart(fig_session_dist, use_container_width=True)
        
        # Session metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Session Duration", f"{df_sessions_filtered['session_duration_hours'].mean():.1f}h")
        col2.metric("Median Session Duration", f"{df_sessions_filtered['session_duration_hours'].median():.1f}h")
        col3.metric("Total Sessions", f"{len(df_sessions_filtered):,}")
    else:
        st.info("Session data not available with current data source.")

# --- Tab 6: Funnel Analysis ---
with tab6:
    st.header("Conversion Funnel Analysis")
    
    if not df_funnel_analysis.empty:
        st.dataframe(df_funnel_analysis, use_container_width=True)
    else:
        st.info("Funnel analysis data not available. This requires dbt mart models to be generated.")
        st.markdown("""
        **To enable funnel analysis:**
        1. Run `cd dbt && dbt run` to generate mart models
        2. Refresh this dashboard
        """)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**🎯 Powered by Modern Data Stack**")
st.sidebar.caption("CSV → Spark → dbt → Streamlit Pipeline")
