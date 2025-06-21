
with raw as (
    select * from {{ source('telemetry', 'feature_usage_hourly') }}
),
with_timestamps as (
    select
        user_id,
        feature,
        window_start as session_time
    from raw
),
first_last as (
    select
        user_id,
        min(session_time) as session_start,
        max(session_time) as session_end,
        count(distinct feature) as feature_count
    from with_timestamps
    group by user_id
)

select * from first_last
