with base as(
    select cast(rider_id as integer) as rider_id
        , cast(signup_date as date) as signup_date
        , cast(country as string) as country
        , cast(referral_code as string) as referral_code
        , cast(created_at as timestamp) as created_at
        , row_number() over(partition by rider_id order by created_at) as rnk
    from {{source('beejanride_raw_dataset', 'riders_raw')}}
)
select rider_id, signup_date, country
, referral_code,  created_at
from base
where rnk = 1
and rider_id is not null
and rider_id >= 0