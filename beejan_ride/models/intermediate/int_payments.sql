select
    p.trip_id
    , sum(case when p.payment_status='success' then {{calculate_net_revenue('p.amount', 'p.fee')}}
         else 0 end) as net_revenue
    , countif(p.payment_status='success') as successful_payments
    , countif(p.payment_status='failed') as failed_payments
    , case when count(*) > 1 then 'Y' else 'N' end as duplicate_payment_flag -- count of payment records per trip_id
  from {{ ref('stg_payments') }} p
  group by p.trip_id