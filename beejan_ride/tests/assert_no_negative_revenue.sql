select * from
{{ref('int_trips_enriched')}}
where net_revenue < 0