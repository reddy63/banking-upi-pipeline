{{
    config(
        materialized = 'ephemeral',
        tags         = ['intermediate'],
        description  = 'Enriched transaction data: adds risk scoring, velocity features, and bank-level flags',
    )
}}

/*
  int_transactions_enriched
  ─────────────────────────
  Adds derived features that multiple mart models need:
    - txn_size_bucket     : 'MICRO' | 'SMALL' | 'MEDIUM' | 'LARGE' | 'HIGH_VALUE'
    - is_cross_bank       : sender and receiver on different banks
    - is_self_transfer    : same user (approximated by same bank handle)
    - is_suspicious       : any fraud-signal flag is true
    - sender_txn_rank     : row_number per sender_vpa ordered by txn_timestamp
    - day_part            : 'NIGHT' | 'MORNING' | 'AFTERNOON' | 'EVENING'
*/

with base as (
    select * from {{ ref('stg_upi_transactions') }}
),

enriched as (
    select
        *,

        -- Transaction size buckets
        case
            when amount_inr < 100         then 'MICRO'
            when amount_inr < 1000        then 'SMALL'
            when amount_inr < 10000       then 'MEDIUM'
            when amount_inr < 50000       then 'LARGE'
            else                               'HIGH_VALUE'
        end                                     as txn_size_bucket,

        -- Cross-bank flag
        (sender_bank != receiver_bank)          as is_cross_bank,

        -- Rough self-transfer detection (same bank, similar name prefix)
        (sender_bank = receiver_bank and
         left(sender_vpa, 3) = left(receiver_vpa, 3)) as is_self_transfer,

        -- Day part classification
        case
            when txn_hour between 0  and 5  then 'NIGHT'
            when txn_hour between 6  and 11 then 'MORNING'
            when txn_hour between 12 and 17 then 'AFTERNOON'
            when txn_hour between 18 and 23 then 'EVENING'
        end                                     as day_part,

        -- Composite suspicious flag (any of the rule-based flags)
        (is_high_value and is_off_hours)        as is_suspicious,

        -- Month and week for time-series aggregations
        extract(month from txn_date)            as txn_month,
        extract(week  from txn_date)            as txn_week,
        extract(year  from txn_date)            as txn_year

    from base
),

with_rank as (
    select
        *,
        row_number() over (
            partition by sender_vpa
            order by txn_timestamp asc
        )                                       as sender_lifetime_rank,

        row_number() over (
            partition by sender_vpa, txn_date
            order by txn_timestamp asc
        )                                       as sender_daily_rank

    from enriched
)

select * from with_rank
