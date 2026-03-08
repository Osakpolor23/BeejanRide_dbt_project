{{ config(
    meta={
      'owner': 'marketing_team',
      'tags': ['operations', 'finance']
    }
) }}

select
    rider_id
    , signup_date
    , country
    , referral_code
    , created_at
    , rider_lifetime_value
    , rider_total_trips
    , count_cities_visited
    , last_ride_date
from {{ ref('int_rider_metrics') }}

