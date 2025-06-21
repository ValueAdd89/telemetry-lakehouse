{{ config(materialized='table') }}

SELECT 
    -- Multiple time granularities pre-calculated
    DATE_TRUNC('hour', window_start) AS hour_start,
    DATE_TRUNC('day', window_start) AS day_start,
    DATE_TRUNC('week', window_start) AS week_start,
    DATE_TRUNC('month', window_start) AS month_start,
    
    user_id,
    feature,
    SUM(event_count) AS event_count
FROM {{ ref('stg_feature_events') }}
GROUP BY 1, 2, 3, 4, 5, 6