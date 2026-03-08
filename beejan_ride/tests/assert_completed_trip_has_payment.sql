select t.trip_id
from {{ref('stg_trips')}} t
where status = 'completed'
and not exists(
select 1
from
{{ref("stg_payments")}} p
where t.trip_id = p.trip_id
and p.payment_status = 'success'
)