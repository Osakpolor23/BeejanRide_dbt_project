with base as
(
    select cast(trip_id as integer) as trip_id
        , cast(rider_id as integer) as rider_id
        , cast(driver_id as integer) as driver_id
        , cast(city_id as integer) as city_id
        , cast(vehicle_id as string) as vehicle_id
        , cast(requested_at as timestamp) as requested_at
        , cast(pickup_at as timestamp) as pickup_at   
        , cast(dropoff_at as timestamp) as dropoff_at
        , cast(status as string) as status
        , cast(estimated_fare as numeric) as estimated_fare
        , cast(actual_fare as numeric) as actual_fare
        , cast(surge_multiplier as numeric) as surge_multiplier
        , cast(payment_method as string) as payment_method
        , cast(is_corporate as bool) as is_corporate
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , row_number() over(partition by trip_id order by created_at) as rnk
    from {{source('beejanride_raw_dataset', 'trips_raw')}}
)

select trip_id, rider_id, driver_id, city_id
, vehicle_id, requested_at, pickup_at, dropoff_at
, status, estimated_fare, actual_fare, surge_multiplier
, payment_method, is_corporate, created_at, updated_at
from base
where rnk = 1
and trip_id is not null
and trip_id >= 0
