select
    d.driver_id
    , d.city_id
    , d.rating
    , sum(f.net_revenue) as total_driver_revenue
from {{ ref('facts_trips') }} f
join {{ ref('dim_drivers') }} d
  on f.driver_id = d.driver_id
where f.status = 'completed'
group by d.driver_id, d.city_id, d.rating
order by total_driver_revenue desc
limit 10

