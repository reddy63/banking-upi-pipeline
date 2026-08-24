{% macro audit_columns() %}
    -- Standard audit columns appended to every mart model
    current_timestamp()                         as _dbt_inserted_at,
    '{{ invocation_id }}'                       as _dbt_invocation_id,
    '{{ this.name }}'                           as _dbt_model,
    '{{ target.name }}'                         as _dbt_target
{% endmacro %}


{% macro get_run_date() %}
    {#- Return the configured run_date variable or today -#}
    cast('{{ var("run_date") }}' as date)
{% endmacro %}


{% macro safe_divide(numerator, denominator, default=0) %}
    {#- Null-safe division -#}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then {{ default }}
        else {{ numerator }}::float / {{ denominator }}::float
    end
{% endmacro %}


{% macro cents_to_rupees(col) %}
    {#- Convert paisa (integer) to INR decimal -#}
    round({{ col }}::numeric / 100.0, 2)
{% endmacro %}


{% macro is_valid_vpa(col) %}
    {#- Returns boolean: whether the VPA matches standard UPI format -#}
    regexp_like({{ col }}, '^[a-zA-Z0-9._-]+@[a-zA-Z]+$')
{% endmacro %}
