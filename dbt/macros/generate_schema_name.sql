{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Override default dbt schema naming.
        In dev: use  <target_schema>_<custom_schema>  (e.g. dbt_dev_staging)
        In prod: use just <custom_schema>              (e.g. staging)
    -#}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
