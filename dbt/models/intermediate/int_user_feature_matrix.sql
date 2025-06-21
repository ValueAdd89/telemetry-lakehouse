-- dbt/models/intermediate/int_user_feature_matrix.sql
{{ config(materialized='table') }}

SELECT 
    user_id,
    feature,
    SUM(event_count) AS total_feature_events,
    COUNT(*) AS feature_usage_sessions,
    MIN(window_start) AS first_feature_use,
    MAX(window_start) AS latest_feature_use,
    EXTRACT(DAYS FROM (MAX(window_start) - MIN(window_start))) AS feature_usage_span_days
FROM {{ ref('stg_feature_events') }}
GROUP BY user_id, feature