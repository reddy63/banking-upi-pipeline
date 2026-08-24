{{
    config(
        materialized = 'table',
        schema       = 'mart_transaction',
        tags         = ['mart', 'transaction', 'daily'],
        description  = 'Daily transaction summary: per-date × bank × status aggregated KPIs',
        unique_key   = ['txn_date', 'sender_bank', 'receiver_bank', 'status', 'device_type'],
    )
}}

/*
  fct_daily_summary
  ──────────────────
  Grain: one row per (txn_date × sender_bank × receiver_bank × status × device_type)
  Primary analytics table for time-series dashboards.

  KPIs:
    - Transaction volume & count
    - Success / failure rates
    - Amount statistics
    - Risk metrics (high-value, off-hours, suspicious counts)
    - Unique sender / receiver counts
*/

with base as (
    select * from {{ ref('int_transactions_enriched') }}
),

summary as (
    select
        txn_date,
        sender_bank,
        receiver_bank,
        status,
        device_type,
        day_part,
        is_weekend,

        -- Volume KPIs
        count(txn_id)                               as txn_count,
        sum(amount_inr)                             as total_amount_inr,
        avg(amount_inr)                             as avg_amount_inr,
        percentile_cont(0.5) within group (
            order by amount_inr
        )                                           as median_amount_inr,
        max(amount_inr)                             as max_amount_inr,
        min(amount_inr)                             as min_amount_inr,

        -- Status breakdown
        sum(case when status = 'SUCCESS'  then 1 else 0 end) as success_count,
        sum(case when status = 'FAILED'   then 1 else 0 end) as failed_count,
        sum(case when status = 'PENDING'  then 1 else 0 end) as pending_count,
        sum(case when status = 'REVERSED' then 1 else 0 end) as reversed_count,

        -- Risk flags
        sum(case when is_high_value   then 1 else 0 end) as high_value_count,
        sum(case when is_off_hours    then 1 else 0 end) as off_hours_count,
        sum(case when is_suspicious   then 1 else 0 end) as suspicious_count,
        sum(case when is_cross_bank   then 1 else 0 end) as cross_bank_count,

        -- Unique parties
        count(distinct sender_vpa)                  as unique_senders,
        count(distinct receiver_vpa)                as unique_receivers,
        count(distinct city)                        as cities_active,

        -- Size buckets
        sum(case when txn_size_bucket = 'MICRO'      then 1 else 0 end) as micro_count,
        sum(case when txn_size_bucket = 'SMALL'      then 1 else 0 end) as small_count,
        sum(case when txn_size_bucket = 'MEDIUM'     then 1 else 0 end) as medium_count,
        sum(case when txn_size_bucket = 'LARGE'      then 1 else 0 end) as large_count,
        sum(case when txn_size_bucket = 'HIGH_VALUE' then 1 else 0 end) as high_value_bucket_count

    from base
    group by
        txn_date, sender_bank, receiver_bank, status,
        device_type, day_part, is_weekend
),

with_rates as (
    select
        s.*,
        {{ safe_divide('s.success_count',   's.txn_count') }} as success_rate,
        {{ safe_divide('s.failed_count',    's.txn_count') }} as failure_rate,
        {{ safe_divide('s.suspicious_count','s.txn_count') }} as risk_rate,
        {{ audit_columns() }}
    from summary s
)

select * from with_rates
