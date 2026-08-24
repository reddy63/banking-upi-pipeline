{{
    config(
        materialized = 'table',
        tags         = ['customer', 'daily'],
        description  = 'Current state of customers for snapshotting',
        unique_key   = 'sender_vpa',
    )
}}

/*
  dim_customers
  ─────────────
  Grain: one row per unique sender_vpa (UPI customer identity)
  Populated from ALL historical Silver data (full refresh).

  Key attributes:
    - Home city (modal city)
    - Primary device type
    - Primary bank
    - First / last seen dates
    - Account tier (based on total spend)
*/

with txns as (
    select * from {{ ref('int_transactions_enriched') }}
),

-- City mode per VPA
city_rank as (
    select
        sender_vpa,
        city,
        count(*)                                    as city_count,
        row_number() over (
            partition by sender_vpa
            order by count(*) desc
        )                                           as rnk
    from txns
    where city is not null
    group by sender_vpa, city
),

home_city as (
    select sender_vpa, city as home_city
    from city_rank
    where rnk = 1
),

-- Primary device
device_rank as (
    select
        sender_vpa,
        device_type,
        count(*)                                    as device_count,
        row_number() over (
            partition by sender_vpa
            order by count(*) desc
        )                                           as rnk
    from txns
    group by sender_vpa, device_type
),

primary_device as (
    select sender_vpa, device_type as primary_device_type
    from device_rank
    where rnk = 1
),

-- Core customer aggregation
customer_agg as (
    select
        sender_vpa,
        sender_bank                                 as primary_bank,
        min(txn_date)                               as first_txn_date,
        max(txn_date)                               as last_txn_date,
        count(distinct txn_date)                    as active_days,
        count(txn_id)                               as total_txns,
        sum(amount_inr)                             as total_spend_inr,
        avg(amount_inr)                             as avg_txn_amount_inr,
        max(amount_inr)                             as max_single_txn_inr,
        count(distinct receiver_vpa)                as unique_payees,
        count(distinct city)                        as cities_transacted,
        sum(case when status = 'SUCCESS'  then 1 else 0 end)  as success_count,
        sum(case when status = 'FAILED'   then 1 else 0 end)  as failed_count,
        sum(case when status = 'REVERSED' then 1 else 0 end)  as reversed_count,
        sum(case when is_high_value       then 1 else 0 end)  as high_value_count,
        sum(case when is_off_hours        then 1 else 0 end)  as off_hours_count,
        sum(case when is_suspicious       then 1 else 0 end)  as suspicious_txn_count
    from txns
    group by sender_vpa, sender_bank
),

-- Customer tier classification
with_tier as (
    select
        ca.*,
        case
            when ca.total_spend_inr >= 1000000  then 'PLATINUM'
            when ca.total_spend_inr >= 100000   then 'GOLD'
            when ca.total_spend_inr >= 10000    then 'SILVER'
            else                                     'BRONZE'
        end                                     as customer_tier,
        {{ safe_divide('ca.success_count', 'ca.total_txns') }} as success_rate,
        {{ safe_divide('ca.failed_count',  'ca.total_txns') }} as failure_rate
    from customer_agg ca
)

select
    wt.*,
    hc.home_city,
    pd.primary_device_type,
    {{ audit_columns() }}
from with_tier wt
left join home_city     hc using (sender_vpa)
left join primary_device pd using (sender_vpa)
