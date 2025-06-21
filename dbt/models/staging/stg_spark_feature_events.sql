{{ config(
    materialized='view',
    meta={
      'data_source': 'spark_processed',
      'upstream_system': 'Apache Spark hourly_aggregation job'
    }
) }}

-- Clean and validate Spark-processed feature usage data
SELECT 
    CAST(window_start AS TIMESTAMP) AS window_start,
    TRIM(user_id) AS user_id,
    LOWER(TRIM(feature)) AS feature,
    CAST(event_count AS INTEGER) AS event_count,
    
    -- Add data lineage metadata
    'spark_hourly_aggregation' AS source_job,
    CURRENT_TIMESTAMP() AS dbt_processed_at
    
FROM {{ source('spark_telemetry', 'feature_usage_hourly') }}

-- Data quality checks on Spark outputs
WHERE window_start IS NOT NULL
  AND user_id IS NOT NULL
  AND feature IS NOT NULL
  AND event_count > 0
  AND event_count <= 1000  -- Sanity check on Spark aggregation