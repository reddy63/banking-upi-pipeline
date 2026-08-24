{{
    config(
        materialized = 'table',
        schema       = 'mart_transaction',
        tags         = ['mart', 'transaction', 'daily'],
        description  = 'Transaction fact table: enriched, denormalised single row per transaction',
        unique_key   = 'txn_id',
    )
}}

/*
  fct_transactions
  ─────────────────
  Grain: one row per transaction (txn_id)
  The canonical transaction fact for BI dashboards and ad-hoc analysis.

  Includes:
    - All cleansed staging attributes
    - All enriched intermediate features
    - Customer tier from dim_customers (denormalised for query speed)
*/

with txns as (
    select * from {{ ref('int_transactions_enriched') }}
),

customers as (
    select
        customer_vpa as sender_vpa,
        customer_tier,
        total_txns              as customer_lifetime_txns,
        total_spend_inr         as customer_lifetime_spend
    from {{ ref('dim_customers') }}
),

final as (
    select
        -- Core identifiers
        t.txn_id,
        t.upi_ref,

        -- Parties
        t.sender_vpa,
        t.receiver_vpa,
        t.sender_bank,
        t.receiver_bank,

        -- Amount
        t.amount_inr,
        t.currency,
        t.txn_size_bucket,

        -- Status
        t.status,

        -- Time dimensions
        t.txn_timestamp,
        t.txn_date,
        t.txn_hour,
        t.txn_dow,
        t.txn_month,
        t.txn_week,
        t.txn_year,
        t.day_part,
        t.is_weekend,

        -- Device & location
        t.device_type,
        t.device_id,
        t.ip_address,
        t.city,
        t.remarks,

        -- Feature flags
        t.is_upi,
        t.is_high_value,
        t.is_off_hours,
        t.is_cross_bank,
        t.is_self_transfer,
        t.is_suspicious,

        -- Customer context (denormalised)
        c.customer_tier,
        c.customer_lifetime_txns,
        c.customer_lifetime_spend,

        -- Velocity rank (sender's Nth txn of the day)
        t.sender_daily_rank,
        t.sender_lifetime_rank,

        -- Pipeline metadata
        t.ingested_at,
        t._source,
        t._processed_ts,

        -- Audit
        {{ audit_columns() }}

    from txns t
    left join customers c using (sender_vpa)
)

select * from final
