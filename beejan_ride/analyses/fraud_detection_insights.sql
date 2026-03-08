select
    f.trip_id,
    f.city_id,
    f.driver_id,
    f.rider_id,
    f.duplicate_payment_flag,
    f.failed_payment_on_completed_trip,
    f.extreme_surge_flag
from {{ ref('facts_trips') }} f
where f.duplicate_payment_flag = 'Y'
   or f.failed_payment_on_completed_trip = 'Y'
   or f.extreme_surge_flag = 'Y'

