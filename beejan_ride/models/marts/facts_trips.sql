{{ config(
    meta={
      'owner': 'analytics_team',
      'tags': ['operations', 'fraud', 'finance']
         }
         ) 
}}

select
    trip_id
    , city_id
    , driver_id
    , rider_id
    , vehicle_id
    , pickup_at
    , dropoff_at
    , requested_at
    , status
    , payment_method
    , corporate_trip_flag
    , trip_duration_minutes
    , failed_payment_on_completed_trip
    , duplicate_payment_flag
    , successful_payments
    , failed_payments
    , extreme_surge_flag
    , actual_fare
    , net_revenue
from {{ ref('int_trips_enriched') }} 
