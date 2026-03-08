with trips as (
  select *
  from {{ ref('int_trips') }} 
),
payments as (
  select *
  from {{ ref('int_payments') }}  
)
select
  t.*,
  p.net_revenue
  , p.successful_payments
  , p.failed_payments
  , p.duplicate_payment_flag
  , case when t.status = 'completed' and p.successful_payments=0 then 'Y' else 'N' end as failed_payment_on_completed_trip
from trips t
left join payments p
on t.trip_id = p.trip_id
