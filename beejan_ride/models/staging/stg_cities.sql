with base as(
    select cast(city_id as integer) as city_id
        , cast(city_name as string) as city_name
        , cast(country as string) as country
        , cast(launch_date as date) as launch_date
        , row_number() over(partition by city_id order by launch_date) as rnk
    from {{source('beejanride_raw_dataset', 'cities_raw')}}
)
select city_id, city_name, country, launch_date
from base
where rnk = 1
and city_id is not null
and city_id >= 0