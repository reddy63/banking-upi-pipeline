{{
    config(
        materialized = 'table',
        schema       = 'mart_fraud',
        tags         = ['mart', 'fraud', 'daily'],
        description  = 'Fraud summary: per-date per-signal aggregate with volumes and risk counts',
        unique_key   = ['txn_date', 'signal_type'],
    )
}}

/*
  fct_fraud_summary
  ──────────────────
  Grain: one row per (txn_date × signal_type)
  Provides daily fraud monitoring KPIs:
    - How many txns triggered each signal
    - Total risk volume (INR)
    - Unique senders involved
    - Average signal score for the day
  Used by fraud analysts and monitoring dashboards.
*/

with signals as (
    select * from {{ ref('fct_fraud_signals') }}
),

summary as (
    select
        txn_date,
        signal_type,

        -- Count KPIs
        count(txn_id)                               as flagged_txn_count,
        count(distinct sender_vpa)                  as unique_flagged_senders,

        -- Amount KPIs
        sum(amount_inr)                             as total_flagged_amount_inr,
        avg(amount_inr)                             as avg_flagged_amount_inr,
        max(amount_inr)                             as max_flagged_amount_inr,

        -- Score KPIs
        avg(signal_score)                           as avg_signal_score,
        max(signal_score)                           as max_signal_score,

        -- Time distribution
        min(txn_timestamp)                          as earliest_flag_ts,
        max(txn_timestamp)                          as latest_flag_ts

    from signals
    group by txn_date, signal_type
),

-- Add share of total txns for the day
total_txns as (
    select
        txn_date,
        count(txn_id)                               as day_total_txns
    from {{ ref('stg_upi_transactions') }}
    group by txn_date
),

final as (
    select
        s.*,
        t.day_total_txns,
        {{ safe_divide('s.flagged_txn_count', 't.day_total_txns') }} as signal_rate,
        {{ audit_columns() }}
    from summary s
    left join total_txns t using (txn_date)
)

select * from final
order by txn_date desc, flagged_txn_count desc
