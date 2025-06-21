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
        
        funnel_analysis = pd.DataFrame() # Empty for now
        overview_kpis = pd.DataFrame({
            'total_events': [df_feature_events['event_count'].sum()],
            'unique_users': [df_feature_events['user_id'].nunique()],
            'avg_events_per_user': [df_feature_events['event_count'].sum() / df_feature_events['user_id'].nunique()],
            'avg_session_duration_hours': [session_funnel['session_duration_hours'].mean()]
        })
        
        time_aggregations = df_feature_events.copy()
        # Ensure these are datetime objects for .dt accessor
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
        st.error("**Debug Info (CSV Fallback):**")
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
                    for file in csv_files[:5]: # Show first 5
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
        Path(__file__).parent.parent.parent / "dbt" / "target", # Most likely for your structure if dbt output is flat
        Path(__file__).parent.parent.parent / "dbt" / "target" / "run" / "telemetry_lakehouse" / "models" / "marts", # More specific dbt path
        Path(__file__).parent.parent / "target" / "marts", # Another common dbt path if dbt project is sibling
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
                        df_time_aggregations = pd.read_csv(dbt_path / f"mart_feature_usage_by_time{fmt}", parse_dates=["day_start", "week_start", "month_start"]) # Ensure parsing for time_aggregations mart
                        df_funnel_analysis = pd.read_csv(dbt_path / f"mart_funnel_analysis{fmt}", parse_dates=["timestamp"]) if (dbt_path / f"mart_funnel_analysis{fmt}").exists() else pd.DataFrame()
                        
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
            st.info(f"Trying next dbt path or format due to: {e}") # Info, not error, to keep trying
            continue
    
    return None # Return None if no dbt data found after trying all paths/formats

# === Load Data Using Fallback Strategy ===
try:
    dbt_data_loaded = True
    (df_all_data, df_users, df_user_sessions, df_top_features, 
     df_funnel_analysis, df_overview_kpis, df_time_aggregations) = load_data_from_dbt_outputs()
    
    if df_all_data is None: # If dbt loading failed
        dbt_data_loaded = False
        st.warning("Could not load data from dbt outputs. Falling back to CSV files from 'data/spark_processed/'.")
        (df_all_data, df_users, df_user_sessions, df_top_features, 
         df_funnel_analysis, df_overview_kpis, df_time_aggregations) = load_data_from_csv()

except Exception as e:
    st.error(f"""
    🚨 **Critical Error Loading Data**
    
    Error: {str(e)}
    
    **Troubleshooting Steps:**
    1. Check that data files exist in `data/spark_processed/` folder (if using fallback)
    2. If using dbt: Run `cd dbt && dbt run` to generate mart models.
    3. Ensure CSV/Parquet files have correct column names and data types.
    4. Refresh the page.
    """)
    st.stop()


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
    
    **Current Source:** Auto-detected (dbt Marts or CSV Fallback)
    """)

# === Apply Filters to Mart Data ===
if len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) 
else: 
    start_date = pd.to_datetime(min_date_val)
    end_date = pd.to_datetime(max_date_val) + pd.Timedelta(days=1)

# Filter df_all_data (main merged data)
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
        freq_str = 'D'
    elif time_granularity == "Weekly":
        freq_str = 'W'
    else: # Monthly
        freq_str = 'M' # Use 'M' for period.to_timestamp() or .start_time

    # Use pre-aggregated time data from mart_feature_usage_by_time if available, otherwise calculate on-the-fly
    if not df_time_aggregations.empty and f"{freq_str}_start" in df_time_aggregations.columns:
        time_col = f"{freq_str}_start" # Column name in mart_feature_usage_by_time
        
        # Filter df_time_aggregations by date range and user/feature
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
                                     title=f"Total Events ({time_granularity}) - From dbt Mart")
            st.plotly_chart(fig_events_time, use_container_width=True)
        else:
            st.info("No time-aggregated data available for current filters from dbt marts.")
    else:
        st.info("Falling back to on-the-fly time aggregation as dbt mart not available or incomplete.")
        # Fallback to on-the-fly aggregation if mart is not available
        df_filtered['time_group'] = df_filtered['window_start'].dt.to_period(freq_str).dt.start_time if freq_str == 'M' else df_filtered['window_start'].dt.to_period(freq_str).dt.to_timestamp()
        
        events_over_time = df_filtered.groupby('time_group')['event_count'].sum().reset_index()
        events_over_time.columns = ['Date', 'Total Events']

        if not events_over_time.empty:
            fig_events_time = px.line(events_over_time, x="Date", y="Total Events", 
                                     title=f"Total Events ({time_granularity}) - On-the-fly calculation")
            st.plotly_chart(fig_events_time, use_container_width=True)
        else:
            st.info("No event data over time for the current selection (even with fallback).")


# --- Tab 2: Feature Analysis ---
with tab2:
    st.header("Feature Usage Analysis")
    
    if selected_feature == 'All':
        st.info("Displaying trends for all features. To view raw data, select a specific feature from the sidebar.")
        st.markdown(f"##### Usage Trends for All Features")
        if not df_filtered.empty:
            # Aggregate by time_group and feature to avoid too many lines if many users
            df_feature_time_agg = df_filtered.groupby(['time_group', 'feature'])['event_count'].sum().reset_index()
            df_feature_time_agg.columns = ['Date', 'feature', 'event_count'] 
            
            fig_feature_time = px.line(df_feature_time_agg, x="Date", y="event_count", color="feature",
                                   title=f"All Features Usage Over Time ({time_granularity})")
            fig_feature_time.update_layout(xaxis_title=f"Date ({time_granularity})", yaxis_title="Event Count")
            st.plotly_chart(fig_feature_time, use_container_width=True)
        else:
            st.info("No feature data available for trend analysis with current filters.")
    else: # A single feature is selected
        st.markdown(f"##### Usage timeline for: `{selected_feature}`")
        df_single_feature_filtered = df_filtered[df_filtered["feature"] == selected_feature].copy()
        
        if not df_single_feature_filtered.empty:
            # Aggregate by time_group for single feature view
            single_feature_time_agg = df_single_feature_filtered.groupby('time_group')['event_count'].sum().reset_index()
            single_feature_time_agg.columns = ['Date', 'event_count']

            fig_time = px.line(single_feature_time_agg, x="Date", y="event_count",
                            title=f"'{selected_feature}' Usage Over Time ({time_granularity})") 
            st.plotly_chart(fig_time, use_container_width=True)

            st.markdown("##### Raw Data for Selected Feature")
            st.dataframe(df_single_feature_filtered)
        else:
            st.info(f"No data found for feature '{selected_feature}' with current filters.")


# --- Tab 3: User Insights ---
with tab3:
    st.header("User Interaction & Activity")

    if not df_filtered.empty and 'user_id' in df_filtered.columns and 'feature' in df_filtered.columns and 'event_count' in df_filtered.columns:
        st.subheader("User-Feature Interaction Matrix")
        pivot = df_filtered.pivot_table(index="user_id", columns="feature", values="event_count", fill_value=0)
        st.dataframe(pivot.style.background_gradient(axis=1, cmap="Blues"))
    else:
        st.info("Insufficient data to create user-feature interaction matrix for the selected filters.")

    st.markdown("---")
    st.subheader("User Activity Breakdown")

    if selected_user == 'All': # Use selected_user which comes from st.selectbox
        st.info("Displaying overall user activity. Select a specific user from the sidebar for a detailed breakdown.")
        
        if not df_filtered.empty and 'user_id' in df_filtered.columns:
            user_total_events = df_filtered.groupby('user_id')['event_count'].sum().reset_index()
            user_total_events = user_total_events.sort_values('event_count', ascending=False)
            
            # Show top N users by event count
            top_users_by_events = user_total_events.head(top_n_features) # Using top_n_features slider for users too
            
            if not top_users_by_events.empty:
                st.markdown(f"##### Top {top_n_features} Users by Total Events")
                fig_top_users = px.bar(top_users_by_events, x='user_id', y='event_count',
                                      title=f"Top {top_n_features} Users by Total Events (Filtered)", text='event_count')
                fig_top_users.update_traces(texttemplate='%{text:,}', textposition='outside')
                st.plotly_chart(fig_top_users, use_container_width=True)
            else:
                st.info("No top users found for the current filters.")

            st.markdown("##### Distribution of Events Per User")
            fig_user_event_dist = px.histogram(user_total_events, x='event_count', nbins=30,
                                              title='Distribution of Total Events Per User (Filtered)')
            st.plotly_chart(fig_user_event_dist, use_container_width=True)
        else:
            st.info("No user activity data to summarize for all users.")

    else: # A specific user is selected (selected_user)
        st.info(f"Displaying detailed activity for User: `{selected_user}`.")
        user_data = df_filtered[df_filtered["user_id"] == selected_user].copy()
        if not user_data.empty:
            st.markdown(f"#### User: `{selected_user}`")
            
            user_total_events = user_data['event_count'].sum()
            user_unique_features = user_data['feature'].nunique()
            col_user_kpi1, col_user_kpi2 = st.columns(2)
            col_user_kpi1.metric("Total Events", f"{int(user_total_events):,}")
            col_user_kpi2.metric("Unique Features Used", f"{user_unique_features:,}")

            feature_usage_by_user = user_data.groupby('feature')['event_count'].sum().reset_index()
            fig_user_features = px.bar(feature_usage_by_user, x='feature', y='event_count',
                                       title=f"Feature Usage for User {selected_user}", text='event_count')
            fig_user_features.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig_user_features, use_container_width=True)
            
            st.markdown("##### Raw Event Data for User")
            st.dataframe(user_data)
        else:
            st.info(f"No data found for user `{selected_user}` with current filters.")


# --- Tab 4: Top Features ---
with tab4:
    st.header(f"Top {top_n_features} Features by Event Count")
    
    if not df_filtered.empty and 'feature' in df_filtered.columns and 'event_count' in df_filtered.columns:
        # Dynamically calculate top features based on filtered data and slider input
        top_features_filtered = df_filtered.groupby("feature").agg({"event_count": "sum"}).reset_index()
        top_features_filtered = top_features_filtered.sort_values("event_count", ascending=False).head(top_n_features)
        
        if not top_features_filtered.empty:
            st.dataframe(top_features_filtered, use_container_width=True)

            fig_top = px.bar(top_features_filtered, x="feature", y="event_count", title=f"Top {top_n_features} Features by Total Events (Filtered)", text="event_count")
            fig_top.update_traces(texttemplate='%{text:,}', textposition='outside')
            st.plotly_chart(fig_top, use_container_width=True)
        else:
            st.info(f"No top {top_n_features} features found for the current selection.")
    else:
        st.info("No feature event data available to determine top features for the current filters.")

# --- Tab 5: User Session Funnels (Renamed to User Session Insights) ---
with tab5:
    st.header("User Session Insights")

    if not df_sessions_filtered.empty: # Use df_sessions_filtered here
        # Session KPIs
        total_sessions_kpi = df_sessions_filtered.shape[0]
        avg_features_per_session_kpi = df_sessions_filtered['feature_count'].mean() if 'feature_count' in df_sessions_filtered.columns else 0
        avg_session_duration_kpi = df_sessions_filtered['session_duration_hours'].mean() if 'session_duration_hours' in df_sessions_filtered.columns else 0

        col_sess_kpi1, col_sess_kpi2, col_sess_kpi3 = st.columns(3)
        col_sess_kpi1.metric("Total Sessions", f"{total_sessions_kpi:,}")
        col_sess_kpi2.metric("Avg Features per Session", f"{avg_features_per_session_kpi:.1f}")
        col_sess_kpi3.metric("Avg Session Duration (Hours)", f"{avg_session_duration_kpi:.1f}h")


        st.markdown("##### Feature Diversity per User Session (Filtered)")
        fig_sessions = px.scatter(df_sessions_filtered, x="session_start", y="feature_count",
                                  size="total_events", 
                                  color="user_id", title="Feature Diversity per User Session",
                                  hover_data=['session_end', 'total_events', 'session_duration_hours']) 
        st.plotly_chart(fig_sessions, use_container_width=True)

        st.markdown("---") # Separator

        st.markdown("##### Sessions by Duration")
        fig_session_duration = px.histogram(df_sessions_filtered, x="session_duration_hours", nbins=20,
                                            title="Distribution of Session Duration (Hours)")
        st.plotly_chart(fig_session_duration, use_container_width=True)

        st.markdown("---") # Separator

        st.markdown("##### Raw Session Data")
        st.dataframe(df_sessions_filtered)
    else:
        st.info("No session data available for the selected filters.")

# --- Tab 6: Funnel Analysis ---
with tab6:
    st.header("Funnel Analysis")

    # Use the filtered funnel data from load_data_from_dbt_outputs or load_data_from_csv
    funnel_options_map = { 
        "Onboarding Funnel": df_funnel_analysis[df_funnel_analysis['funnel_name'] == 'onboarding'].copy() if 'funnel_name' in df_funnel_analysis.columns else pd.DataFrame(), 
        "Feature Adoption Funnel": df_funnel_analysis[df_funnel_analysis['funnel_name'] == 'feature_adoption'].copy() if 'funnel_name' in df_funnel_analysis.columns else pd.DataFrame(),
        "Workflow Completion Funnel": df_funnel_analysis[df_funnel_analysis['funnel_name'] == 'workflow_completion'].copy() if 'funnel_name' in df_funnel_analysis.columns else pd.DataFrame()
    }
    
    # Filter the specific funnel dataframes by user and date from the global filters
    for key, df in funnel_options_map.items():
        if not df.empty:
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] < end_date)].copy()
            if selected_user != 'All':
                df = df[df['user_id'] == selected_user].copy()
            funnel_options_map[key] = df
        

    selected_funnel_label = st.selectbox("Select Funnel", list(funnel_options_map.keys()))
    df_funnel = funnel_options_map.get(selected_funnel_label) 

    if df_funnel is not None and not df_funnel.empty:
        try:
            # Updated Funnel Step Order Definitions (ensure these match your dbt model output for mart_funnel_analysis)
            if selected_funnel_label == "Onboarding Funnel":
                funnel_step_order = ["Landing Page Visit", "Sign Up Form", "Email Verification", 
                                     "Profile Setup", "Tutorial Start", "First Feature Use", "Onboarding Complete"]
            elif selected_funnel_label == "Feature Adoption Funnel":
                funnel_step_order = ["Feature Discovery", "Feature Click", "Feature Trial", 
                                     "Feature Regular Use", "Feature Mastery", "Feature Advocacy"]
            elif selected_funnel_label == "Workflow Completion Funnel":
                funnel_step_order = ["Workflow Start", "Data Input", "Configuration", 
                                     "Preview Generated", "Validation Passed", "Final Review", "Workflow Complete"]
            else:
                funnel_step_order = df_funnel["funnel_step"].unique().tolist() 

            
            funnel_counts = df_funnel.groupby("funnel_step")["user_id"].nunique().reset_index()
            
            funnel_counts['funnel_step'] = pd.Categorical(funnel_counts['funnel_step'], categories=funnel_step_order, ordered=True)
            funnel_counts = funnel_counts.sort_values("funnel_step", ascending=True) 

            funnel_counts = funnel_counts.dropna(subset=['funnel_step'])

            funnel_counts.columns = ["Funnel Step", "User Count"]

            if not funnel_counts.empty: 
                st.subheader(f"{selected_funnel_label} - Drop-off Summary")
                st.dataframe(funnel_counts)

                fig_funnel = go.Figure(go.Funnel(
                    y=funnel_counts["Funnel Step"],
                    x=funnel_counts["User Count"],
                    textinfo="value+percent previous+percent initial"
                ))
                fig_funnel.update_layout(title=f"{selected_funnel_label} Visualization")
                st.plotly_chart(fig_funnel, use_container_width=True)
            else:
                st.info(f"No data available for {selected_funnel_label} with the current filters and step order definitions.")

        except pd.errors.ParserError:
            st.error(f"Error: Could not parse the funnel data for {selected_funnel_label}. Please ensure it's a valid DataFrame with 'user_id', 'funnel_step', and 'timestamp' columns.")
        except KeyError as e:
            st.error(f"Error: Missing expected column in funnel data: {e}. Please ensure 'user_id', 'funnel_step', and 'timestamp' exist.")
        except Exception as e:
            st.error(f"An unexpected error occurred while processing funnel data: {e}")
    else:
        st.info(f"No funnel data available for {selected_funnel_label} with the current filters. Please check your data or adjust filters.")
