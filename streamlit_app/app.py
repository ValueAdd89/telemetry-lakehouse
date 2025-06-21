
import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(layout="wide", page_title="Telemetry Explorer")

# Load CSV simulating dbt models
feature_events = pd.read_csv("warehouse/feature_usage_hourly/sample.csv", parse_dates=["window_start"])

# Simulated top features (for now, based on same input)
top_features = feature_events.groupby("feature").agg({"event_count": "sum"}).reset_index()
top_features = top_features.sort_values("event_count", ascending=False).head(10)

# Simulated session funnel from raw input
session_funnel = (
    feature_events.groupby("user_id")
    .agg(session_start=("window_start", "min"),
         session_end=("window_start", "max"),
         feature_count=("feature", "nunique"))
    .reset_index()
)

st.title("📊 Telemetry Lakehouse Dashboard")
st.markdown("Use this dashboard to explore product feature usage, user behavior, and engagement over time.")

# Tabs for UX
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Overview", 
    "🔍 Feature Analysis", 
    "👥 User Insights", 
    "🏆 Top Features", 
    "⏱ Session Funnels"
])

with tab1:
    st.header("Product Usage Summary")
    col1, col2 = st.columns(2)
    col1.metric("Total Events", int(feature_events['event_count'].sum()))
    col2.metric("Unique Users", feature_events['user_id'].nunique())

    fig_total = px.bar(feature_events.groupby("feature").event_count.sum().reset_index(),
                       x="feature", y="event_count", title="Total Events by Feature")
    st.plotly_chart(fig_total, use_container_width=True)

with tab2:
    st.header("Feature-Level Analysis")
    selected_feature = st.selectbox("Select a Feature", feature_events["feature"].unique())
    df_feature = feature_events[feature_events["feature"] == selected_feature]

    st.markdown(f"##### Usage timeline for: `{selected_feature}`")
    fig_time = px.line(df_feature, x="window_start", y="event_count", color="user_id",
                       title=f"{selected_feature} Usage Over Time")
    st.plotly_chart(fig_time, use_container_width=True)

    st.markdown("##### Raw Data")
    st.dataframe(df_feature)

with tab3:
    st.header("User Interaction Matrix")
    pivot = feature_events.pivot_table(index="user_id", columns="feature", values="event_count", fill_value=0)
    st.dataframe(pivot.style.background_gradient(axis=1, cmap="Blues"))

    selected_user = st.selectbox("Inspect a specific user", feature_events["user_id"].unique())
    user_data = feature_events[feature_events["user_id"] == selected_user]
    st.markdown(f"##### Events for User `{selected_user}`")
    st.dataframe(user_data)

with tab4:
    st.header("Top Features")
    st.dataframe(top_features)

    fig_top = px.bar(top_features, x="feature", y="event_count", title="Top 10 Features by Total Events")
    st.plotly_chart(fig_top, use_container_width=True)

with tab5:
    st.header("User Session Funnels")
    st.dataframe(session_funnel)

    fig_sessions = px.scatter(session_funnel, x="session_start", y="feature_count",
                              size=session_funnel["feature_count"],
                              color="user_id", title="Feature Diversity per User Session")
    st.plotly_chart(fig_sessions, use_container_width=True)
