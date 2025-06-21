
with events as (
    select * from {{ ref('feature_events') }}
)

select
    feature,
    sum(hourly_events) as total_events
from events
group by feature
order by total_events desc
limit 10
