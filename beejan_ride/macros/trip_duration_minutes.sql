{% macro trip_duration_minutes(pickup_at, dropoff_at) %}
    TIMESTAMP_DIFF({{ dropoff_at }}, {{ pickup_at }}, MINUTE)
{% endmacro %}
