{% test assert_positive(model, column_name) %}
    select *
    from {{ model }}
    where coalesce({{ column_name }}, 0) < 0
{% endtest %}
