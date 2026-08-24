{{
    config(
        materialized = 'table',
        schema       = 'mart_customer',
        tags         = ['mart', 'customer', 'daily'],
        description  = 'Customer daily activity fact: per-VPA per-day transaction summary',
        unique_key   = ['sender_vpa', 'txn_date'],
    )
}}

/*
  fct_customer_activity
  ─────────────────────
  Grain: one row per sender_vpa per txn_date
  Joins to dim_customers for customer attributes.

  KPIs:
    - Daily transaction count & volume
    - Success / failure rates
    - Unique payees for the day
    - Risk score (normalised suspicious flag rate)
*/

with txns as (
    select * from {{ ref('int_transactions_enriched') }}
),

-- Daily activity rollup per customer
daily as (
    select
        sender_vpa,
        txn_date,
        sender_bank,
        count(txn_id)                               as daily_txn_count,
        sum(amount_inr)                             as daily_total_inr,
        avg(amount_inr)                             as daily_avg_inr,
        max(amount_inr)                             as daily_max_inr,
        count(distinct receiver_vpa)                as daily_unique_payees,
        count(distinct device_type)                 as device_types_used,
        count(distinct city)                        as cities_used,
        sum(case when status = 'SUCCESS'  then 1 else 0 end)  as success_count,
        sum(case when status = 'FAILED'   then 1 else 0 end)  as failed_count,
        sum(case when status = 'REVERSED' then 1 else 0 end)  as reversed_count,
        sum(case when is_high_value       then 1 else 0 end)  as high_value_count,
        sum(case when is_off_hours        then 1 else 0 end)  as off_hours_count,
        sum(case when is_suspicious       then 1 else 0 end)  as suspicious_count,
        sum(case when is_cross_bank       then 1 else 0 end)  as cross_bank_count,
        min(txn_timestamp)                          as first_txn_ts,
        max(txn_timestamp)                          as last_txn_ts,
        -- Time span of activity in minutes
        extract(epoch from (max(txn_timestamp) - min(txn_timestamp))) / 60
                                                    as activity_span_minutes
    from txns
    group by sender_vpa, txn_date, sender_bank
),

with_rates as (
    select
        d.*,
        {{ safe_divide('d.success_count',   'd.daily_txn_count') }} as daily_success_rate,
        {{ safe_divide('d.failed_count',    'd.daily_txn_count') }} as daily_failure_rate,
        {{ safe_divide('d.suspicious_count','d.daily_txn_count') }} as daily_risk_score,
        {{ audit_columns() }}
    from daily d
)

select * from with_rates
