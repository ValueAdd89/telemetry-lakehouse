
# dbt Models for Telemetry Lakehouse

This folder contains curated dbt models built on top of Spark-transformed raw telemetry data.
These models support product usage analytics, user journey tracking, and feature performance insights.

Models:
- `feature_events.sql`: Aggregated hourly user-feature interactions
- `top_features.sql`: Top 10 most used features overall
- `session_funnels.sql`: User-level session duration and feature engagement
