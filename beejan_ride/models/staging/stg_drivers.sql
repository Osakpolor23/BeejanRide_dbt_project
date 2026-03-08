with base as(
    select cast(driver_id as integer) as driver_id
        , cast(city_id as integer) as city_id
        , cast(onboarding_date as date) as onboarding_date
        , cast(driver_status as string) as driver_status
        , cast(vehicle_id as string) as vehicle_id
        , cast(rating as numeric) as rating
        , cast(created_at as timestamp) as created_at
        , cast(updated_at as timestamp) as updated_at
        , row_number() over(partition by driver_id order by created_at) as rnk
    from {{source('beejanride_raw_dataset', 'drivers_raw')}}
)
select driver_id, city_id, onboarding_date
, driver_status, vehicle_id, rating
, created_at, updated_at
from base
where rnk = 1
and driver_id is not null
and driver_id >= 0