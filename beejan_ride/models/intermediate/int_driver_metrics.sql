with driver_trips as(
select
  d.driver_id
  , count(t.trip_id) as total_trips
  , avg(actual_fare) as avg_trip_value
  , min(requested_at) as first_trip_at
  , max(requested_at) as last_trip_at
  , sum(t.actual_fare) as gross_revenue
from {{ ref('stg_drivers') }} d
left join {{ ref('stg_trips') }} t
on d.driver_id = t.driver_id
where status = 'completed'
group by d.driver_id
),
driver_revenue as (
    select
        t.driver_id,
        sum({{calculate_net_revenue('p.amount', 'p.fee')}}) as net_revenue
    from {{ ref('stg_trips') }} t
    left join {{ ref('stg_payments') }} p
      on t.trip_id = p.trip_id
    where t.status = 'completed'
      and p.payment_status = 'success'
    group by t.driver_id
)

select d.driver_id
    , d.city_id
    , d.driver_status
    , d.rating
    , d.vehicle_id
    , d.onboarding_date
    , d.created_at
    , d.updated_at
    , coalesce(dt.total_trips, 0) as driver_lifetime_trips
    , coalesce(dt.avg_trip_value, 0) as avg_trip_value
    , coalesce(dt.gross_revenue, 0) as gross_revenue
    , coalesce(dr.net_revenue, 0) as net_revenue
    , dt.first_trip_at
    , dt.last_trip_at
    from {{ref('stg_drivers')}} d
    left join driver_trips dt
    on d.driver_id = dt.driver_id
    left join driver_revenue dr
    on d.driver_id = dr.driver_id