with base as(
    select cast(payment_id as integer) as payment_id
        , cast(trip_id as integer) as trip_id
        , cast(payment_status as string) as payment_status
        , cast(payment_provider as string) as payment_provider
        , cast(amount as numeric) as amount
        , cast(fee as numeric) as fee
        , cast(currency as string) as currency
        , cast(created_at as timestamp) as created_at
        , row_number() over(partition by payment_id order by created_at) as rnk
    from {{source('beejanride_raw_dataset', 'payments_raw')}}
)
select payment_id, trip_id, payment_status
, payment_provider, amount, fee
, currency, created_at
from base
where rnk = 1
and payment_id is not null
and payment_id >= 0 