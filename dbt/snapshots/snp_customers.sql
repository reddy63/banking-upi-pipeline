{% snapshot snp_customers %}

{{
    config(
      target_database=env_var('SNOWFLAKE_DATABASE', 'BANKING_DW'),
      target_schema='snapshots',
      unique_key='sender_vpa',
      strategy='check',
      check_cols=['primary_bank', 'customer_tier', 'home_city', 'primary_device_type']
    )
}}

select * from {{ ref('int_customers_current') }}

{% endsnapshot %}
