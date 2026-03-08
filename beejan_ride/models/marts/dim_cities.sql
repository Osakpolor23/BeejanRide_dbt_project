{{ config(
    meta={
      'owner': 'analytics_team',
      'tags': ['operations']
    }
) }}

select
    city_id,
    city_name,
    country,
    launch_date,
from {{ ref('stg_cities') }}
