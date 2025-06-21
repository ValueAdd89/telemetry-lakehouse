{{ config(materialized='table') }}

-- Pre-calculated KPIs for dashboard Overview tab
SELECT 
    COUNT(DISTINCT s.user_id) AS total_unique_users,
    SUM(s.total_events) AS total_events,
    AVG(s.total_events) AS avg_events_per_user,
    AVG(s.feature_count) AS avg_features_per_user,
    AVG(s.session_duration_hours) AS avg_session_duration_hours
FROM {{ ref('int_user_sessions') }} s