select
    t.*,
    {{ trip_duration_minutes('t.pickup_at', 't.dropoff_at') }} as trip_duration_minutes,
    case when t.is_corporate then 'Y' else 'N' end as corporate_trip_flag,
    case when t.surge_multiplier > 10 then 'Y' else 'N' end as extreme_surge_flag,
  from {{ ref('stg_trips') }} t