{{ config(materialized='table') }}

SELECT 
    feature,
    SUM(event_count) AS total_events,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT DATE_TRUNC('day', window_start)) AS active_days,
    AVG(event_count) AS avg_events_per_occurrence,
    ROUND(SUM(event_count)::DECIMAL / COUNT(DISTINCT user_id), 2) AS events_per_user
FROM {{ ref('stg_feature_events') }}
GROUP BY feature