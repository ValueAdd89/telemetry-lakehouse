
with raw as (
    select * from {{ source('telemetry', 'feature_usage_hourly') }}
)

select
    user_id,
    feature,
    count(*) as hourly_events
from raw
group by user_id, feature
