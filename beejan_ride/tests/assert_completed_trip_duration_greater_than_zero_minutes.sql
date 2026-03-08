select trip_id
, TIMESTAMP_DIFF(dropoff_at, pickup_at, MINUTE) AS trip_duration_minutes
from {{ref('stg_trips')}}
where status = 'completed'
and TIMESTAMP_DIFF(dropoff_at, pickup_at, MINUTE) <= 0
