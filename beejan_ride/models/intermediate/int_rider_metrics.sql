with rider_trips as(
select
  r.rider_id
  , sum(t.actual_fare) as rider_lifetime_value
  , count(t.trip_id) as total_trips
  , count(distinct city_id) as count_cities_visited
  , max(t.requested_at) as last_ride_date
from {{ ref('stg_riders') }} r
left join {{ ref('stg_trips') }} t
on r.rider_id = t.rider_id
where t.status = 'completed'
group by r.rider_id
)

select r.rider_id
    , r.signup_date
    , r.country
    , r.referral_code
    , r.created_at
    , coalesce(rt.rider_lifetime_value, 0) as rider_lifetime_value
    , coalesce(rt.total_trips, 0) as rider_total_trips
    , count_cities_visited
    , last_ride_date
from {{ref('stg_riders')}} r
left join rider_trips rt
on r.rider_id = rt.rider_id