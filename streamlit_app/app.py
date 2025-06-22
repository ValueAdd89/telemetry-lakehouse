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

# --- Custom CSS for rounded card design ---
st.markdown("""
<style>
.metric-card {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1rem;
    border-radius: 20px;
    color: white;
    margin: 0.5rem 0;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
}

.chart-container {
    background: white;
    padding: 1rem;
    border-radius: 20px;
    margin: 0.5rem 0;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    border: 1px solid #f0f0f0;
}

div.stMetric {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1rem;
    border-radius: 15px;
    color: white;
    text-align: center;
}

div.stMetric > label {
    color: white !important;
    font-weight: 600;
}

div.stMetric > div {
    color: white !important;
    font-weight: 700;
}

.stPlotlyChart > div {
    background: white;
    border-radius: 20px;
    padding: 10px;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    border: 1px solid #f0f0f0;
}

.stDataFrame > div {
    background: white;
    border-radius: 20px;
    padding: 10px;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    border: 1px solid #f0f0f0;
}

.gradient-card-1 {
    background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
    padding: 1.5rem;
    border-radius: 20px;
    color: white;
    margin: 0.5rem 0;
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
}

.gradient-card-2 {
    background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
    padding: 1.5rem;
    border-radius: 20px;
    color: #333;
    margin: 0.5rem 0;
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
}

.gradient-card-3 {
    background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
    padding: 1.5rem;
    border-radius: 20px;
    color: white;
    margin: 0.5rem 0;
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
}

.gradient-card-4 {
    background: linear-gradient(135deg, #55efc4 0%, #00b894 100%);
    padding: 1.5rem;
    border-radius: 20px;
    color: white;
    margin: 0.5rem 0;
    box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
}
</style>
""", unsafe_allow_html=True)

st.title("📊 Telemetry Lakehouse Dashboard")
st.markdown("Use this dashboard to explore product feature usage, user behavior, and engagement over time.")
st.markdown("---")

# --- Data Loading Functions ---
@st.cache_data
def load_data_from_csv():
    """
    Fallback: Load data from original CSV files in spark_processed directory
    """
    # Fixed path to point to spark_processed directory
    base_path = Path(__file__).parent.parent / "data" / "spark_processed"

    try:
        # Load original CSV files as fallback
        df_feature_events = pd.read_csv(base_path / "feature_usage_hourly_sample.csv", parse_dates=["window_start"])
        df_users = pd.read_csv(base_path / "users.csv")
        
        # Optional funnel files - don't fail if they don't exist
        try:
            df_funnel_onboarding = pd.read_csv(base_path / "funnel_onboarding.csv", parse_dates=["timestamp"])
            df_funnel_feature_adoption = pd.read_csv(base_path / "funnel_feature_adoption.csv", parse_dates=["timestamp"])
            df_funnel_workflow_completion = pd.read_csv(base_path / "funnel_workflow_completion.csv", parse_dates=["timestamp"])
        except FileNotFoundError:
            # These are optional - create empty dataframes if not found
            df_funnel_onboarding = pd.DataFrame()
            df_funnel_feature_adoption = pd.DataFrame()
            df_funnel_workflow_completion = pd.DataFrame()

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
            # Look for different file formats
            formats_to_try = ['.csv', '.parquet', '.json']
            
            for fmt in formats_to_try:
                feature_file = dbt_path / f"mart_feature_usage_hourly{fmt}"
                if feature_file.exists():
                    
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
tab_names = ["📈 Overview", "🔍 Feature Analysis", "👥 User Insights", "🏆 Top Features", "⏱ Session Analysis", "📉 Funnel Analysis", "📊 Feature & Workflow Breakdown"]
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(tab_names)

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

    # Create time groupings for charts
    if time_granularity == "Daily":
        freq = 'D'
    elif time_granularity == "Weekly":
        freq = 'W'
    else:
        freq = 'MS'

    df_filtered['time_group'] = df_filtered['window_start'].dt.to_period(freq).dt.to_timestamp()
    
    # Grid layout for visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Events Over Time")
        events_over_time = df_filtered.groupby('time_group')['event_count'].sum().reset_index()
        events_over_time.columns = ['Date', 'Total Events']

        if not events_over_time.empty:
            fig_events_time = px.line(events_over_time, x="Date", y="Total Events", 
                                    title=f"Total Events ({time_granularity})")
            fig_events_time.update_layout(height=400)
            st.plotly_chart(fig_events_time, use_container_width=True)
        else:
            st.info("No event data over time for the current selection.")
    
    with col2:
        st.subheader("Top Features")
        feature_summary = df_filtered.groupby('feature')['event_count'].sum().reset_index()
        feature_summary = feature_summary.sort_values('event_count', ascending=False).head(5)
        
        if not feature_summary.empty:
            fig_feature_pie = px.pie(feature_summary, values="event_count", names="feature", 
                                   title="Top 5 Features Distribution")
            fig_feature_pie.update_layout(height=400)
            st.plotly_chart(fig_feature_pie, use_container_width=True)
        else:
            st.info("No feature data available.")

    # Second row
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("User Activity Distribution")
        user_activity = df_filtered.groupby('user_id')['event_count'].sum().reset_index()
        user_activity.columns = ['user_id', 'total_events']
        
        if not user_activity.empty:
            fig_user_dist = px.histogram(user_activity, x="total_events", nbins=15,
                                       title="User Activity Levels")
            fig_user_dist.update_layout(height=400)
            st.plotly_chart(fig_user_dist, use_container_width=True)
        else:
            st.info("No user activity data available.")
    
    with col4:
        st.subheader("Feature Usage Trends")
        if not df_filtered.empty:
            # Show top 3 features only for readability
            top_features_list = df_filtered.groupby('feature')['event_count'].sum().nlargest(3).index.tolist()
            feature_time = df_filtered.groupby(['time_group', 'feature'])['event_count'].sum().reset_index()
            feature_time_filtered = feature_time[feature_time['feature'].isin(top_features_list)]
            
            if not feature_time_filtered.empty:
                fig_feature_trends = px.line(feature_time_filtered, x="time_group", y="event_count", 
                                           color="feature", title="Top 3 Features Over Time")
                fig_feature_trends.update_layout(height=400)
                st.plotly_chart(fig_feature_trends, use_container_width=True)
            else:
                st.info("No feature trend data available.")
        else:
            st.info("No data available for feature trends.")

# --- Tab 2: Feature Analysis ---
with tab2:
    st.header("Feature Usage Analysis")
    
    # Create time groupings
    if time_granularity == "Daily":
        freq = 'D'
    elif time_granularity == "Weekly":
        freq = 'W'
    else:
        freq = 'MS'
    
    df_filtered['time_group'] = df_filtered['window_start'].dt.to_period(freq).dt.to_timestamp()
    
    # Grid layout for feature analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Feature Usage Trends")
        feature_time = df_filtered.groupby(['time_group', 'feature'])['event_count'].sum().reset_index()
        
        if not feature_time.empty:
            # Show top features only for readability
            top_features_list = df_filtered.groupby('feature')['event_count'].sum().nlargest(top_n_features).index.tolist()
            feature_time_filtered = feature_time[feature_time['feature'].isin(top_features_list)]
            
            fig_feature_trends = px.line(feature_time_filtered, x="time_group", y="event_count", 
                                       color="feature", title=f"Top {top_n_features} Feature Usage Over Time")
            fig_feature_trends.update_layout(height=450)
            st.plotly_chart(fig_feature_trends, use_container_width=True)
        else:
            st.info("No feature trend data available.")
    
    with col2:
        st.subheader("Feature Usage Distribution")
        feature_summary = df_filtered.groupby('feature')['event_count'].sum().reset_index()
        feature_summary = feature_summary.sort_values('event_count', ascending=False).head(top_n_features)
        
        if not feature_summary.empty:
            fig_feature_bar = px.bar(feature_summary, x="feature", y="event_count", 
                                   title=f"Top {top_n_features} Features by Usage")
            fig_feature_bar.update_xaxes(tickangle=45)
            fig_feature_bar.update_layout(height=450)
            st.plotly_chart(fig_feature_bar, use_container_width=True)
        else:
            st.info("No feature distribution data available.")

    # Second row - Feature heatmap and statistics
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("Feature Usage by Hour")
        if not df_filtered.empty and 'window_start' in df_filtered.columns:
            df_filtered['hour'] = df_filtered['window_start'].dt.hour
            hourly_usage = df_filtered.groupby(['hour', 'feature'])['event_count'].sum().reset_index()
            
            if not hourly_usage.empty:
                # Create pivot for heatmap
                hourly_pivot = hourly_usage.pivot(index='feature', columns='hour', values='event_count')
                hourly_pivot = hourly_pivot.fillna(0)
                
                fig_heatmap = px.imshow(hourly_pivot, 
                                      title="Feature Usage Heatmap by Hour",
                                      labels=dict(x="Hour of Day", y="Feature", color="Event Count"))
                fig_heatmap.update_layout(height=450)
                st.plotly_chart(fig_heatmap, use_container_width=True)
            else:
                st.info("No hourly usage data available.")
        else:
            st.info("No timestamp data available for hourly analysis.")
    
    with col4:
        st.subheader("Feature Statistics")
        if not df_filtered.empty:
            feature_stats = df_filtered.groupby('feature').agg({
                'event_count': ['sum', 'mean', 'std'],
                'user_id': 'nunique'
            }).round(2)
            
            feature_stats.columns = ['Total Events', 'Avg Events', 'Std Dev', 'Unique Users']
            feature_stats = feature_stats.sort_values('Total Events', ascending=False).head(10)
            
            st.dataframe(feature_stats, use_container_width=True, height=450)
        else:
            st.info("No feature statistics available.")

# --- Tab 3: User Insights ---
with tab3:
    st.header("User Behavior Analysis")

    # Grid layout for user insights
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("User Activity Distribution")
        user_activity = df_filtered.groupby('user_id')['event_count'].sum().reset_index()
        user_activity.columns = ['user_id', 'total_events']
        
        if not user_activity.empty:
            fig_user_dist = px.histogram(user_activity, x="total_events", nbins=20,
                                       title="Distribution of User Activity Levels")
            fig_user_dist.update_layout(height=400)
            st.plotly_chart(fig_user_dist, use_container_width=True)
        else:
            st.info("No user activity data available.")
    
    with col2:
        st.subheader("Most Active Users")
        if not user_activity.empty:
            top_users = user_activity.nlargest(10, 'total_events')
            
            fig_top_users = px.bar(top_users, x="user_id", y="total_events",
                                 title="Top 10 Most Active Users")
            fig_top_users.update_layout(height=400)
            st.plotly_chart(fig_top_users, use_container_width=True)
        else:
            st.info("No user data available.")

    # Second row
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("User-Feature Interaction Matrix")
        if not df_filtered.empty and 'user_id' in df_filtered.columns and 'feature' in df_filtered.columns and 'event_count' in df_filtered.columns:
            # Sample top users and features for readability
            top_users_sample = df_filtered.groupby('user_id')['event_count'].sum().nlargest(10).index.tolist()
            top_features_sample = df_filtered.groupby('feature')['event_count'].sum().nlargest(5).index.tolist()
            
            df_sample = df_filtered[
                (df_filtered['user_id'].isin(top_users_sample)) & 
                (df_filtered['feature'].isin(top_features_sample))
            ]
            
            if not df_sample.empty:
                pivot = df_sample.pivot_table(index="user_id", columns="feature", values="event_count", fill_value=0)
                
                fig_heatmap = px.imshow(pivot, 
                                      title="User-Feature Interaction Heatmap (Top Users & Features)",
                                      labels=dict(x="Feature", y="User ID", color="Event Count"))
                fig_heatmap.update_layout(height=400)
                st.plotly_chart(fig_heatmap, use_container_width=True)
            else:
                st.info("Insufficient data for interaction matrix.")
        else:
            st.info("Insufficient data to create user-feature interaction matrix.")
    
    with col4:
        st.subheader("User Engagement Segmentation")
        if not df_filtered.empty:
            # Create user segments based on activity
            user_features = df_filtered.groupby('user_id').agg({
                'feature': 'nunique',
                'event_count': 'sum'
            }).reset_index()
            user_features.columns = ['user_id', 'unique_features', 'total_events']
            
            # Define segments
            def categorize_user(row):
                if row['unique_features'] >= 5 and row['total_events'] >= 20:
                    return 'Power User'
                elif row['unique_features'] >= 3 and row['total_events'] >= 10:
                    return 'Active User'
                elif row['unique_features'] >= 2 and row['total_events'] >= 5:
                    return 'Regular User'
                else:
                    return 'Casual User'
            
            user_features['segment'] = user_features.apply(categorize_user, axis=1)
            segment_counts = user_features['segment'].value_counts().reset_index()
            segment_counts.columns = ['Segment', 'Users']
            
            fig_segments = px.pie(segment_counts, values='Users', names='Segment',
                                title="User Engagement Segments")
            fig_segments.update_layout(height=400)
            st.plotly_chart(fig_segments, use_container_width=True)
        else:
            st.info("No data available for user segmentation.")

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
        # Grid layout for session analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Session Duration Distribution")
            fig_session_dist = px.histogram(df_sessions_filtered, x="session_duration_hours", nbins=20,
                                          title="Distribution of Session Durations")
            fig_session_dist.update_layout(height=400)
            st.plotly_chart(fig_session_dist, use_container_width=True)
        
        with col2:
            st.subheader("Session Metrics Overview")
            # Create a summary chart
            metrics_data = {
                'Metric': ['Avg Duration', 'Median Duration', 'Max Duration', 'Min Duration'],
                'Hours': [
                    df_sessions_filtered['session_duration_hours'].mean(),
                    df_sessions_filtered['session_duration_hours'].median(),
                    df_sessions_filtered['session_duration_hours'].max(),
                    df_sessions_filtered['session_duration_hours'].min()
                ]
            }
            metrics_df = pd.DataFrame(metrics_data)
            
            fig_metrics = px.bar(metrics_df, x='Metric', y='Hours',
                               title='Session Duration Metrics')
            fig_metrics.update_layout(height=400)
            st.plotly_chart(fig_metrics, use_container_width=True)
        
        # Second row
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Session Length Categories")
            # Categorize sessions
            def categorize_session(duration):
                if duration < 0.5:
                    return 'Quick (< 30min)'
                elif duration < 2:
                    return 'Short (30min - 2h)'
                elif duration < 5:
                    return 'Medium (2h - 5h)'
                else:
                    return 'Long (> 5h)'
            
            df_sessions_filtered['category'] = df_sessions_filtered['session_duration_hours'].apply(categorize_session)
            category_counts = df_sessions_filtered['category'].value_counts().reset_index()
            category_counts.columns = ['Category', 'Count']
            
            fig_categories = px.pie(category_counts, values='Count', names='Category',
                                  title='Session Length Distribution')
            fig_categories.update_layout(height=400)
            st.plotly_chart(fig_categories, use_container_width=True)
        
        with col4:
            st.subheader("Key Session Statistics")
            # Display key metrics in a more visual way
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Avg Session Duration", f"{df_sessions_filtered['session_duration_hours'].mean():.1f}h")
            col_b.metric("Median Session Duration", f"{df_sessions_filtered['session_duration_hours'].median():.1f}h")
            col_c.metric("Total Sessions", f"{len(df_sessions_filtered):,}")
            
            # Additional statistics table
            st.write("**Detailed Statistics:**")
            stats_data = {
                'Statistic': ['Mean', 'Median', 'Mode', 'Std Dev', 'Min', 'Max', '25th Percentile', '75th Percentile'],
                'Value (hours)': [
                    f"{df_sessions_filtered['session_duration_hours'].mean():.2f}",
                    f"{df_sessions_filtered['session_duration_hours'].median():.2f}",
                    f"{df_sessions_filtered['session_duration_hours'].mode().iloc[0] if not df_sessions_filtered['session_duration_hours'].mode().empty else 'N/A'}",
                    f"{df_sessions_filtered['session_duration_hours'].std():.2f}",
                    f"{df_sessions_filtered['session_duration_hours'].min():.2f}",
                    f"{df_sessions_filtered['session_duration_hours'].max():.2f}",
                    f"{df_sessions_filtered['session_duration_hours'].quantile(0.25):.2f}",
                    f"{df_sessions_filtered['session_duration_hours'].quantile(0.75):.2f}"
                ]
            }
            stats_df = pd.DataFrame(stats_data)
            st.dataframe(stats_df, use_container_width=True, height=250)
            
    else:
        st.info("Session data not available with current data source.")

# --- Tab 6: Funnel Analysis ---
with tab6:
    st.header("Conversion Funnel Analysis")
    
    # Create fallback funnel analysis using available CSV data
    try:
        # Load funnel data from CSV files
        base_path = Path(__file__).parent.parent / "data" / "spark_processed"
        
        # Try to load funnel CSV files
        try:
            df_onboarding = pd.read_csv(base_path / "funnel_onboarding.csv", parse_dates=["timestamp"])
            df_feature_adoption = pd.read_csv(base_path / "funnel_feature_adoption.csv", parse_dates=["timestamp"])
            df_workflow_completion = pd.read_csv(base_path / "funnel_workflow_completion.csv", parse_dates=["timestamp"])
            
            # Filter by date range
            df_onboarding_filtered = df_onboarding[
                (df_onboarding['timestamp'] >= start_date) &
                (df_onboarding['timestamp'] < end_date)
            ]
            df_feature_adoption_filtered = df_feature_adoption[
                (df_feature_adoption['timestamp'] >= start_date) &
                (df_feature_adoption['timestamp'] < end_date)
            ]
            df_workflow_completion_filtered = df_workflow_completion[
                (df_workflow_completion['timestamp'] >= start_date) &
                (df_workflow_completion['timestamp'] < end_date)
            ]
            
            # Apply user filter if specific user selected
            if selected_user != 'All':
                df_onboarding_filtered = df_onboarding_filtered[df_onboarding_filtered['user_id'] == selected_user]
                df_feature_adoption_filtered = df_feature_adoption_filtered[df_feature_adoption_filtered['user_id'] == selected_user]
                df_workflow_completion_filtered = df_workflow_completion_filtered[df_workflow_completion_filtered['user_id'] == selected_user]
            
            # Create funnel selector
            funnel_options = {
                "Onboarding Funnel": df_onboarding_filtered,
                "Feature Adoption Funnel": df_feature_adoption_filtered,
                "Workflow Completion Funnel": df_workflow_completion_filtered
            }
            
            selected_funnel = st.selectbox("Select Funnel Type", list(funnel_options.keys()))
            selected_funnel_data = funnel_options[selected_funnel]
            
            if len(selected_funnel_data) > 0:
                
                # Group by funnel step and count unique users
                funnel_analysis = selected_funnel_data.groupby(['funnel_step', 'step_order'])['user_id'].nunique().reset_index()
                funnel_analysis = funnel_analysis.sort_values('step_order')
                funnel_analysis.columns = ['Step', 'Order', 'Users']
                
                # Calculate conversion rates
                total_users_started = funnel_analysis['Users'].iloc[0] if len(funnel_analysis) > 0 else 0
                funnel_analysis['Conversion Rate (%)'] = (funnel_analysis['Users'] / total_users_started * 100) if total_users_started > 0 else 0
                
                # Calculate drop-off rates
                funnel_analysis['Drop-off Rate (%)'] = 100 - funnel_analysis['Conversion Rate (%)']
                
                # Display funnel table
                st.subheader(f"{selected_funnel} Analysis")
                st.dataframe(funnel_analysis[['Step', 'Users', 'Conversion Rate (%)', 'Drop-off Rate (%)']], use_container_width=True)
                
                # Funnel chart
                fig_funnel = px.funnel(funnel_analysis, x='Users', y='Step', 
                                     title=f'{selected_funnel} - User Progression')
                st.plotly_chart(fig_funnel, use_container_width=True)
                
                # Conversion rate trend
                fig_conversion = px.bar(funnel_analysis, x='Step', y='Conversion Rate (%)',
                                      title=f'{selected_funnel} - Conversion Rates by Step')
                fig_conversion.update_xaxes(tickangle=45)
                st.plotly_chart(fig_conversion, use_container_width=True)
                
            else:
                st.warning(f"No data available for {selected_funnel} with the current filters. Please check your data or adjust filters.")
                
        except FileNotFoundError as e:
            st.warning(f"Funnel CSV files not found: {e}. Showing basic user progression analysis.")
            
            # Basic funnel using feature usage data
            user_progression = df_filtered.groupby('user_id').agg({
                'feature': 'nunique',
                'event_count': 'sum',
                'window_start': ['min', 'max']
            }).reset_index()
            
            user_progression.columns = ['user_id', 'unique_features', 'total_events', 'first_activity', 'last_activity']
            user_progression['session_duration'] = (user_progression['last_activity'] - user_progression['first_activity']).dt.total_seconds() / 3600
            
            # Create simple funnel based on engagement levels
            basic_funnel = []
            total_users = user_progression['user_id'].nunique()
            
            # Light users (1-2 features)
            light_users = len(user_progression[user_progression['unique_features'] <= 2])
            basic_funnel.append({
                'step': 'Light Usage (1-2 features)',
                'users': light_users,
                'conversion_rate': (light_users / total_users * 100) if total_users > 0 else 0
            })
            
            # Medium users (3-5 features)
            medium_users = len(user_progression[
                (user_progression['unique_features'] >= 3) & 
                (user_progression['unique_features'] <= 5)
            ])
            basic_funnel.append({
                'step': 'Medium Usage (3-5 features)',
                'users': medium_users,
                'conversion_rate': (medium_users / total_users * 100) if total_users > 0 else 0
            })
            
            # Heavy users (6+ features)
            heavy_users = len(user_progression[user_progression['unique_features'] >= 6])
            basic_funnel.append({
                'step': 'Heavy Usage (6+ features)',
                'users': heavy_users,
                'conversion_rate': (heavy_users / total_users * 100) if total_users > 0 else 0
            })
            
            basic_funnel_df = pd.DataFrame(basic_funnel)
            st.dataframe(basic_funnel_df, use_container_width=True)
            
            # Basic funnel chart
            fig_basic_funnel = px.bar(basic_funnel_df, x='step', y='users',
                                    title='User Engagement Levels')
            fig_basic_funnel.update_xaxes(tickangle=45)
            st.plotly_chart(fig_basic_funnel, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error creating funnel analysis: {e}")
        st.info("Unable to generate funnel analysis with available data.")

# --- Tab 7: Feature & Workflow Breakdown ---
with tab7:
    st.header("Feature & Workflow Breakdown Analysis")
    
    try:
        # Load data for breakdown analysis
        base_path = Path(__file__).parent.parent / "data" / "spark_processed"
        
        try:
            df_feature_adoption = pd.read_csv(base_path / "funnel_feature_adoption.csv", parse_dates=["timestamp"])
            df_workflow_completion = pd.read_csv(base_path / "funnel_workflow_completion.csv", parse_dates=["timestamp"])
            
            # Filter by date range
            df_feature_adoption_filtered = df_feature_adoption[
                (df_feature_adoption['timestamp'] >= start_date) &
                (df_feature_adoption['timestamp'] < end_date)
            ]
            df_workflow_completion_filtered = df_workflow_completion[
                (df_workflow_completion['timestamp'] >= start_date) &
                (df_workflow_completion['timestamp'] < end_date)
            ]
            
            # Apply user filter if specific user selected
            if selected_user != 'All':
                df_feature_adoption_filtered = df_feature_adoption_filtered[df_feature_adoption_filtered['user_id'] == selected_user]
                df_workflow_completion_filtered = df_workflow_completion_filtered[df_workflow_completion_filtered['user_id'] == selected_user]
            
            # Grid layout - Row 1: Feature Analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="gradient-card-1"><h3>🎯 Feature Adoption Overview</h3></div>', unsafe_allow_html=True)
                
                if len(df_feature_adoption_filtered) > 0 and 'feature_name' in df_feature_adoption_filtered.columns:
                    feature_breakdown = df_feature_adoption_filtered.groupby('feature_name')['user_id'].nunique().reset_index()
                    feature_breakdown.columns = ['Feature', 'Users']
                    feature_breakdown = feature_breakdown.sort_values('Users', ascending=False)
                    
                    # Feature adoption chart
                    fig_feature_breakdown = px.bar(feature_breakdown, x='Feature', y='Users',
                                                 title='Users Who Adopted Each Feature',
                                                 color='Users',
                                                 color_continuous_scale='Viridis')
                    fig_feature_breakdown.update_xaxes(tickangle=45)
                    fig_feature_breakdown.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig_feature_breakdown, use_container_width=True)
                else:
                    st.info("No feature adoption data available for breakdown analysis.")
            
            with col2:
                st.markdown('<div class="gradient-card-2"><h3>📊 Feature Funnel Performance</h3></div>', unsafe_allow_html=True)
                
                if len(df_feature_adoption_filtered) > 0:
                    # Feature adoption by funnel step
                    feature_step_breakdown = df_feature_adoption_filtered.groupby(['feature_name', 'funnel_step'])['user_id'].nunique().reset_index()
                    feature_step_breakdown.columns = ['Feature', 'Funnel Step', 'Users']
                    
                    if not feature_step_breakdown.empty:
                        fig_feature_step = px.bar(feature_step_breakdown, x='Feature', y='Users', color='Funnel Step',
                                                title='Feature Adoption by Funnel Step',
                                                color_discrete_sequence=px.colors.qualitative.Set3)
                        fig_feature_step.update_xaxes(tickangle=45)
                        fig_feature_step.update_layout(height=400)
                        st.plotly_chart(fig_feature_step, use_container_width=True)
                    else:
                        st.info("No funnel step data available.")
                else:
                    st.info("No feature adoption data available.")

            # Grid layout - Row 2: Workflow Analysis
            col3, col4 = st.columns(2)
            
            with col3:
                st.markdown('<div class="gradient-card-3"><h3>🔄 Workflow Completion Analysis</h3></div>', unsafe_allow_html=True)
                
                if len(df_workflow_completion_filtered) > 0 and 'workflow_id' in df_workflow_completion_filtered.columns:
                    workflow_breakdown = df_workflow_completion_filtered.groupby('workflow_id')['user_id'].nunique().reset_index()
                    workflow_breakdown.columns = ['Workflow', 'Users']
                    workflow_breakdown = workflow_breakdown.sort_values('Users', ascending=False)
                    
                    # Workflow completion chart
                    fig_workflow_breakdown = px.bar(workflow_breakdown, x='Workflow', y='Users',
                                                  title='Users Who Completed Each Workflow',
                                                  color='Users',
                                                  color_continuous_scale='Plasma')
                    fig_workflow_breakdown.update_xaxes(tickangle=45)
                    fig_workflow_breakdown.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig_workflow_breakdown, use_container_width=True)
                else:
                    st.info("No workflow completion data available.")
            
            with col4:
                st.markdown('<div class="gradient-card-4"><h3>⚡ Workflow Funnel Insights</h3></div>', unsafe_allow_html=True)
                
                if len(df_workflow_completion_filtered) > 0:
                    # Workflow completion by funnel step
                    workflow_step_breakdown = df_workflow_completion_filtered.groupby(['workflow_id', 'funnel_step'])['user_id'].nunique().reset_index()
                    workflow_step_breakdown.columns = ['Workflow', 'Funnel Step', 'Users']
                    
                    if not workflow_step_breakdown.empty:
                        fig_workflow_step = px.bar(workflow_step_breakdown, x='Workflow', y='Users', color='Funnel Step',
                                                 title='Workflow Completion by Funnel Step',
                                                 color_discrete_sequence=px.colors.qualitative.Pastel)
                        fig_workflow_step.update_xaxes(tickangle=45)
                        fig_workflow_step.update_layout(height=400)
                        st.plotly_chart(fig_workflow_step, use_container_width=True)
                    else:
                        st.info("No workflow funnel data available.")
                else:
                    st.info("No workflow data available.")

            # Grid layout - Row 3: Summary Statistics
            col5, col6 = st.columns(2)
            
            with col5:
                st.subheader("📈 Feature Adoption Summary")
                if len(df_feature_adoption_filtered) > 0 and 'feature_name' in df_feature_adoption_filtered.columns:
                    feature_summary = df_feature_adoption_filtered.groupby('feature_name').agg({
                        'user_id': 'nunique',
                        'funnel_step': lambda x: x.nunique()
                    }).reset_index()
                    feature_summary.columns = ['Feature', 'Unique Users', 'Funnel Steps']
                    feature_summary = feature_summary.sort_values('Unique Users', ascending=False)
                    
                    st.dataframe(feature_summary, use_container_width=True, height=300)
                else:
                    st.info("No feature summary data available.")
            
            with col6:
                st.subheader("🎯 Workflow Completion Summary")
                if len(df_workflow_completion_filtered) > 0 and 'workflow_id' in df_workflow_completion_filtered.columns:
                    workflow_summary = df_workflow_completion_filtered.groupby('workflow_id').agg({
                        'user_id': 'nunique',
                        'funnel_step': lambda x: x.nunique()
                    }).reset_index()
                    workflow_summary.columns = ['Workflow', 'Unique Users', 'Funnel Steps']
                    workflow_summary = workflow_summary.sort_values('Unique Users', ascending=False)
                    
                    st.dataframe(workflow_summary, use_container_width=True, height=300)
                else:
                    st.info("No workflow summary data available.")
                
        except FileNotFoundError as e:
            st.warning(f"Feature/Workflow breakdown files not found: {e}")
            
            # Fallback to basic feature analysis from main data with grid layout
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="gradient-card-1"><h3>📊 Basic Feature Usage</h3></div>', unsafe_allow_html=True)
                
                if not df_filtered.empty and 'feature' in df_filtered.columns:
                    basic_feature_breakdown = df_filtered.groupby('feature')['user_id'].nunique().reset_index()
                    basic_feature_breakdown.columns = ['Feature', 'Unique Users']
                    basic_feature_breakdown = basic_feature_breakdown.sort_values('Unique Users', ascending=False)
                    
                    fig_basic_features = px.bar(basic_feature_breakdown, x='Feature', y='Unique Users',
                                              title='Unique Users by Feature (from main data)',
                                              color='Unique Users',
                                              color_continuous_scale='Blues')
                    fig_basic_features.update_xaxes(tickangle=45)
                    fig_basic_features.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig_basic_features, use_container_width=True)
                else:
                    st.info("No feature data available for basic analysis.")
            
            with col2:
                st.markdown('<div class="gradient-card-2"><h3>📋 Feature Statistics</h3></div>', unsafe_allow_html=True)
                
                if not df_filtered.empty and 'feature' in df_filtered.columns:
                    feature_stats = df_filtered.groupby('feature').agg({
                        'user_id': 'nunique',
                        'event_count': ['sum', 'mean']
                    }).round(2)
                    feature_stats.columns = ['Unique Users', 'Total Events', 'Avg Events']
                    feature_stats = feature_stats.sort_values('Total Events', ascending=False)
                    
                    st.dataframe(feature_stats, use_container_width=True, height=400)
                else:
                    st.info("No feature statistics available.")
            
    except Exception as e:
        st.error(f"Error creating breakdown analysis: {e}")
        st.info("Unable to generate breakdown analysis with available data.")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**🎯 Powered by Modern Data Stack**")
st.sidebar.caption("CSV → Spark → dbt → Streamlit Pipeline")
