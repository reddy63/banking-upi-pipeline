{{
    config(
        materialized = 'view',
        schema       = 'mart_customer',
        tags         = ['mart', 'customer', 'daily'],
        description  = 'SCD Type 2 Customer dimension: tracks historical changes to customer profile',
    )
}}

select
    sender_vpa as customer_vpa,
    primary_bank,
    first_txn_date,
    last_txn_date,
    total_txns,
    total_spend_inr,
    customer_tier,
    home_city,
    primary_device_type,
    success_rate,
    failure_rate,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snp_customers') }}
