{{
    config(
        materialized = 'incremental',
        schema = 'staging',
        unique_key = 'event_id'
    )
}}

with base as(
    select cast(event_id as integer) as event_id
        , cast(driver_id as integer) as driver_id
        , cast(status as string) as status
        , cast(event_timestamp as timestamp) as event_timestamp
        , row_number() over(partition by event_id order by event_timestamp) as rnk
    from {{source('beejanride_raw_dataset', 'driver_status_events_raw')}}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses > to include records arriving later or on the same day as the last run of this model)
  where event_timestamp > (select max(event_timestamp) from {{ this }})

{% endif %}
)
select event_id, driver_id, status, event_timestamp
from base
where rnk = 1
and event_id is not null
and event_id >= 0