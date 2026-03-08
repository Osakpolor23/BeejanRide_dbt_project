{% macro calculate_net_revenue(actual_fare, fee) %}
    ({{ actual_fare}} - {{ fee }})
{% endmacro %}