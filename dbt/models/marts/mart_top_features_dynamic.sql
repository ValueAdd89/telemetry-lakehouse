{{ config(materialized='table') }}

-- Pre-ranked features for Top Features tab
SELECT 
    feature,
    total_events,
    unique_users,
    events_per_user,
    ROW_NUMBER() OVER (ORDER BY total_events DESC) AS rank,
    ROUND(total_events * 100.0 / SUM(total_events) OVER (), 2) AS percentage_of_total
FROM {{ ref('int_feature_popularity') }}
ORDER BY total_events DESC