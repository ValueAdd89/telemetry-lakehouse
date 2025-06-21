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
st.markdown("---") # Separator for cleaner look

# --- Data Loading (Cached for performance) ---
@st.cache_data
def load_data():
    base_path = Path(__file__).parent.parent.parent / "data" 

    try:
        # Load core data
        df_feature_events = pd.read_csv(base_path / "feature_usage_hourly_sample.csv", parse_dates=["window_start"])
        df_users = pd.read_csv(base_path / "users.csv")
        
        # Load funnel data
        df_funnel_onboarding = pd.read_csv(base_path / "funnel_onboarding.csv", parse_dates=["timestamp"])
        df_funnel_feature_adoption = pd.read_csv(base_path / "funnel_feature_adoption.csv", parse_dates=["timestamp"])
        df_funnel_workflow_completion = pd.read_csv(base_path / "funnel_workflow_completion.csv", parse_dates=["timestamp"])


        # --- Data Cleaning/Preparation ---
        df_feature_events['window_start'] = pd.to_datetime(df_feature_events['window_start'], errors='coerce')
        df_feature_events.dropna(subset=['window_start'], inplace=True)

        if 'user_id' not in df_users.columns:
            st.error("Error: 'user_id' column not found in users.csv. Cannot merge without a common user identifier.")
            st.stop()
        if 'user_id' not in df_feature_events.columns:
            st.warning("Warning: 'user_id' not found in feature_events. Assigning dummy user_id.")
            df_feature_events['user_id'] = 'unknown_user' 
        
        df_all_data_merged = pd.merge(df_feature_events, df_users, on='user_id', how='left')


        # --- Prepare Session Funnel Data ---
        session_funnel = (
            df_all_data_merged.groupby("user_id") 
            .agg(session_start=("window_start", "min"),
                 session_end=("window_start", "max"),
                 feature_count=("feature", "nunique"),
                 total_events=("event_count", "sum")) 
            .reset_index()
        )
        session_funnel['session_duration_hours'] = (session_funnel['session_end'] - session_funnel['session_start']).dt.total_seconds() / 3600
        
        return df_all_data_merged, df_users, session_funnel, df_funnel_onboarding, df_funnel_feature_adoption, df_funnel_workflow_completion
    except FileNotFoundError as e:
        st.error(f"Required data file not found: {e.filename}. Please ensure all CSVs are in the '{base_path.name}/' directory as specified in the README.")
        st.stop()
    except Exception as e:
        st.error(f"An unexpected error occurred during data loading: {e}. Please check your CSV file contents and column names.")
        st.stop()

# Load all dataframes
df_all_data, users_orig, session_funnel_orig, funnel_onboarding_orig, funnel_feature_adoption_orig, funnel_workflow_completion_orig = load_data()


# --- Sidebar Filters ---
st.sidebar.header("Filter Data")

# Date Range Filter
if not df_all_data['window_start'].empty:
    min_date_val = df_all_data['window_start'].min().date()
    max_date_val = df_all_data['window_start'].max().date()
else:
    min_date_val = pd.to_datetime('2024-01-01').date() 
    max_date_val = pd.to_datetime('2024-01-01').date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date_val, max_date_val),
    min_value=min_date_val,
    max_value=max_date_val
)

if len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1) 
else: 
    start_date = pd.to_datetime(min_date_val)
    end_date = pd.to_datetime(max_date_val) + pd.Timedelta(days=1)

# Granularity Selector
time_granularity = st.sidebar.selectbox(
    "Select Time Granularity",
    options=["Daily", "Weekly", "Monthly"]
)

# --- REFACTOR: Feature Single-Select Dropdown ---
all_features_options = ['All'] + df_all_data['feature'].unique().tolist()
selected_feature = st.sidebar.selectbox("Select Feature", options=all_features_options, index=0)

# --- REFACTOR: User Single-Select Dropdown ---
all_users_options = ['All'] + df_all_data['user_id'].unique().tolist()
selected_user = st.sidebar.selectbox("Select User", options=all_users_options, index=0)

# Top N Features Slider
top_n_features = st.sidebar.slider("Show Top N Features", min_value=5, max_value=20, value=10)


# --- Apply Global Filters to DataFrames ---
df_filtered = df_all_data[
    (df_all_data['window_start'] >= start_date) &
    (df_all_data['window_start'] < end_date) 
].copy() 

# Apply Feature Filter (if not 'All')
if selected_feature != 'All':
    df_filtered = df_filtered[df_filtered['feature'] == selected_feature].copy()

# Apply User Filter (if not 'All')
if selected_user != 'All':
    df_filtered = df_filtered[df_filtered['user_id'] == selected_user].copy()


# Filter session_funnel based on selected users and dates
# session_funnel is filtered by user_id
filtered_session_funnel = session_funnel_orig[
    (session_funnel_orig['session_start'] >= start_date) &
    (session_funnel_orig['session_start'] < end_date)
].copy()
if selected_user != 'All': # Only filter session_funnel by user if a specific user is selected
    filtered_session_funnel = filtered_session_funnel[filtered_session_funnel['user_id'] == selected_user].copy()

# --- REFACTOR: Helper function for filtering funnel data by user (single select) ---
def filter_funnel_by_user(df, user_id):
    if user_id == 'All':
        return df
    else:
        return df[df['user_id'] == user_id].copy()

# Prepare filtered funnel datasets for the Funnel Analysis tab
# These are filtered by date and selected user, but NOT by 'feature' selection
filtered_funnels = {
    "Onboarding Funnel": filter_funnel_by_user(
        funnel_onboarding_orig[(funnel_onboarding_orig['timestamp'] >= start_date) & (funnel_onboarding_orig['timestamp'] < end_date)], 
        selected_user),
    "Feature Adoption Funnel": filter_funnel_by_user(
        funnel_feature_adoption_orig[(funnel_feature_adoption_orig['timestamp'] >= start_date) & (funnel_feature_adoption_orig['timestamp'] < end_date)], 
        selected_user),
    "Workflow Completion Funnel": filter_funnel_by_user(
        funnel_workflow_completion_orig[(funnel_workflow_completion_orig['timestamp'] >= start_date) & (funnel_workflow_completion_orig['timestamp'] < end_date)], 
        selected_user)
}


# Check if filtered data is empty
if df_filtered.empty:
    st.warning("No data available for the selected filters. Please adjust your selections.")
    st.stop()


# --- Dashboard Tabs ---
tab_names = ["📈 Overview", "🔍 Feature Analysis", "👥 User Insights", "🏆 Top Features", "⏱ Session Funnels", "📉 Funnel Analysis"]
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_names)

# --- Tab 1: Dashboard Overview ---
with tab1:
    st.header("Product Usage Summary")

    # KPIs for Overview
    total_events_kpi = df_filtered['event_count'].sum()
    unique_users_kpi = df_filtered['user_id'].nunique()
    
    # Ensure denominator is not zero
    avg_events_per_user = total_events_kpi / unique_users_kpi if unique_users_kpi > 0 else 0
    avg_features_per_session = filtered_session_funnel['feature_count'].mean() if not filtered_session_funnel.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Events", f"{int(total_events_kpi):,}")
    col2.metric("Unique Users", f"{unique_users_kpi:,}")
    col3.metric("Avg Events / User", f"{avg_events_per_user:.1f}")
    col4.metric("Avg Features / Session", f"{avg_features_per_session:.1f}")

    st.markdown("---") # Separator

    st.subheader("Total Events Over Time")
    # Aggregate data by selected granularity
    if time_granularity == "Daily":
        freq = 'D'
    elif time_granularity == "Weekly":
        freq = 'W'
    else: # Monthly
        freq = 'MS' # Month start for better grouping

    # Create a time_group column for aggregation
    df_filtered['time_group'] = df_filtered['window_start'].dt.to_period(freq).dt.to_timestamp()
    
    events_over_time = df_filtered.groupby('time_group')['event_count'].sum().reset_index()
    events_over_time.columns = ['Date', 'Total Events']

    if not events_over_time.empty:
        fig_events_time = px.line(events_over_time, x="Date", y="Total Events", title=f"Total Events ({time_granularity})")
        st.plotly_chart(fig_events_time, use_container_width=True)
    else:
        st.info("No event data over time for the current selection.")

# --- Tab 2: Feature Analysis ---
with tab2:
    st.header("Feature-Level Analysis")
    
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
                            title=f"{selected_feature} Usage Over Time ({time_granularity})")
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

    if selected_user == 'All':
        st.info("Displaying overall user activity. Select a specific user from the sidebar for a detailed breakdown.")
        
        # --- REFACTOR: Show summary visualization for ALL users ---
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

    else: # A specific user is selected
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

    if not filtered_session_funnel.empty:
        # Session KPIs
        total_sessions_kpi = filtered_session_funnel.shape[0]
        avg_features_per_session_kpi = filtered_session_funnel['feature_count'].mean()
        avg_session_duration_kpi = filtered_session_funnel['session_duration_hours'].mean()

        col_sess_kpi1, col_sess_kpi2, col_sess_kpi3 = st.columns(3)
        col_sess_kpi1.metric("Total Sessions", f"{total_sessions_kpi:,}")
        col_sess_kpi2.metric("Avg Features per Session", f"{avg_features_per_session_kpi:.1f}")
        col_sess_kpi3.metric("Avg Session Duration (Hours)", f"{avg_session_duration_kpi:.1f}")


        st.markdown("##### Feature Diversity per User Session (Filtered)")
        fig_sessions = px.scatter(filtered_session_funnel, x="session_start", y="feature_count",
                                  size="total_events", 
                                  color="user_id", title="Feature Diversity per User Session",
                                  hover_data=['session_end', 'total_events', 'session_duration_hours']) 
        st.plotly_chart(fig_sessions, use_container_width=True)

        st.markdown("---") # Separator

        st.markdown("##### Sessions by Duration")
        fig_session_duration = px.histogram(filtered_session_funnel, x="session_duration_hours", nbins=20,
                                            title="Distribution of Session Duration (Hours)")
        st.plotly_chart(fig_session_duration, use_container_width=True)

        st.markdown("---") # Separator

        st.markdown("##### Raw Session Data")
        st.dataframe(filtered_session_funnel)
    else:
        st.info("No session data available for the selected filters.")

# --- Tab 6: Funnel Analysis ---
with tab6:
    st.header("Funnel Analysis")

    funnel_options_map = { # Renamed to avoid clash with variable funnel_options below
        "Onboarding Funnel": filtered_funnels["Onboarding Funnel"], 
        "Feature Adoption Funnel": filtered_funnels["Feature Adoption Funnel"],
        "Workflow Completion Funnel": filtered_funnels["Workflow Completion Funnel"]
    }

    selected_funnel_label = st.selectbox("Select Funnel", list(funnel_options_map.keys()))
    df_funnel = funnel_options_map.get(selected_funnel_label) 

    if df_funnel is not None and not df_funnel.empty:
        try:
            # --- REFACTOR: Updated Funnel Step Order Definitions ---
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
                funnel_step_order = df_funnel["funnel_step"].unique().tolist() # Default to alphabetical if not defined

            
            funnel_counts = df_funnel.groupby("funnel_step")["user_id"].nunique().reset_index()
            
            # Ensure correct ordering of steps
            funnel_counts['funnel_step'] = pd.Categorical(funnel_counts['funnel_step'], categories=funnel_step_order, ordered=True)
            funnel_counts = funnel_counts.sort_values("funnel_step", ascending=True) 

            # Filter out steps not present in the filtered data for this specific funnel
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
