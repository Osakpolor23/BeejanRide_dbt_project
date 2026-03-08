{{ config(
    meta={
      'owner': 'driver_ops_team',
      'tags': ['operations', 'finance']
    }
) }}

select
    driver_id,
    city_id,
    driver_status,
    rating,
    vehicle_id,
    onboarding_date,
    created_at,
    updated_at,
    driver_lifetime_trips,
    avg_trip_value,
    gross_revenue,
    net_revenue,
    first_trip_at,
    last_trip_at
from {{ ref('int_driver_metrics') }}
