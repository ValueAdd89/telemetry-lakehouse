{{ config(materialized='table') }}

WITH user_session_metrics AS (
    SELECT 
        user_id,
        MIN(window_start) AS session_start,
        MAX(window_start) AS session_end,
        COUNT(DISTINCT feature) AS feature_count,
        SUM(event_count) AS total_events,
        COUNT(*) AS hourly_periods_active
    FROM {{ ref('stg_feature_events') }}
    GROUP BY user_id
)

SELECT 
    user_id,
    session_start,
    session_end,
    feature_count,
    total_events,
    hourly_periods_active,
    EXTRACT(EPOCH FROM (session_end - session_start)) / 3600.0 AS session_duration_hours
FROM user_session_metrics