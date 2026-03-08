{{ config(
    materialized='ephemeral'
    ) 
}}
select cast(driver_id as integer) as driver_id
    , cast(city_id as integer) as city_id
    , cast(onboarding_date as date) as onboarding_date
    , cast(driver_status as string) as driver_status
    , cast(vehicle_id as string) as vehicle_id
    , cast(rating as numeric) as rating
    , FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at) as created_at
    , FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', updated_at) as updated_at
    from {{source('beejanride_raw_dataset', 'drivers_raw')}}
where driver_id is not null
and driver_id >= 0