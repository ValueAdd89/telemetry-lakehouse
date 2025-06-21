-- dbt/models/intermediate/int_funnel_conversions.sql
{{ config(materialized='table') }}

WITH funnel_step_counts AS (
    SELECT 
        'onboarding' AS funnel_type,
        funnel_step,
        step_order,
        COUNT(DISTINCT user_id) AS users_at_step
    FROM {{ ref('stg_funnel_onboarding') }}
    GROUP BY funnel_step, step_order
    
    UNION ALL
    
    SELECT 
        'feature_adoption' AS funnel_type,
        funnel_step,
        step_order,
        COUNT(DISTINCT user_id) AS users_at_step
    FROM {{ ref('stg_funnel_feature_adoption') }}
    GROUP BY funnel_step, step_order
    
    UNION ALL
    
    SELECT 
        'workflow_completion' AS funnel_type,
        funnel_step,
        step_order,
        COUNT(DISTINCT user_id) AS users_at_step
    FROM {{ ref('stg_funnel_workflow') }}
    GROUP BY funnel_step, step_order
),

funnel_with_conversions AS (
    SELECT 
        funnel_type,
        funnel_step,
        step_order,
        users_at_step,
        LAG(users_at_step) OVER (
            PARTITION BY funnel_type 
            ORDER BY step_order
        ) AS users_at_previous_step,
        ROUND(
            users_at_step * 100.0 / NULLIF(LAG(users_at_step) OVER (
                PARTITION BY funnel_type 
                ORDER BY step_order
            ), 0), 2
        ) AS step_conversion_rate
    FROM funnel_step_counts
)

SELECT * FROM funnel_with_conversions