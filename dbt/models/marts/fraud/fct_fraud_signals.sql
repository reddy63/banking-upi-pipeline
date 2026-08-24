{{
    config(
        materialized = 'table',
        schema       = 'mart_fraud',
        tags         = ['mart', 'fraud', 'daily'],
        description  = 'Fraud signal fact: one row per transaction per triggered rule',
        unique_key   = ['txn_id', 'signal_type'],
    )
}}

/*
  fct_fraud_signals
  ──────────────────
  Grain: one row per (txn_id, signal_type)
  A single transaction can trigger multiple signals (rows).

  Rule-based signals implemented:
    1. HIGH_VALUE_OFF_HOURS  — amount > 50K, hour 0–5 AM
    2. RAPID_REPEAT_SENDER   — 3+ txns from same sender within 5 mins
    3. ALWAYS_FAILED_SENDER  — sender with 100% failure rate (≥5 txns/day)
    4. CROSS_BANK_HIGH_VALUE — large cross-bank transfer to a new payee
    5. UNUSUAL_CITY          — sender transacting from a new city
    6. SUSPICIOUS_REVERSAL   — reversed txn was high value

  Each signal carries:
    - signal_score  (0–1 risk weight)
    - evidence JSON blob
*/

with txns as (
    select * from {{ ref('int_transactions_enriched') }}
),

-- ── Signal 1: High value off-hours ───────────────────────────────────────────
signal_1 as (
    select
        txn_id,
        sender_vpa,
        txn_date,
        amount_inr,
        txn_timestamp,
        'HIGH_VALUE_OFF_HOURS'                      as signal_type,
        0.85                                        as signal_score,
        'amount > 50K AND hour 0-5'                 as signal_reason
    from txns
    where is_high_value
      and txn_hour between 0 and 5
),

-- ── Signal 2: Rapid repeat sender (3+ txns in same hour) ─────────────────────
hourly_counts as (
    select
        sender_vpa,
        txn_date,
        txn_hour,
        count(txn_id)                               as hourly_txn_count
    from txns
    group by sender_vpa, txn_date, txn_hour
    having count(txn_id) >= 3
),

signal_2 as (
    select
        t.txn_id,
        t.sender_vpa,
        t.txn_date,
        t.amount_inr,
        t.txn_timestamp,
        'RAPID_REPEAT_SENDER'                       as signal_type,
        0.75                                        as signal_score,
        concat('sender sent ', hc.hourly_txn_count::varchar, ' txns in 1 hour') as signal_reason
    from txns t
    inner join hourly_counts hc
        on t.sender_vpa = hc.sender_vpa
       and t.txn_date   = hc.txn_date
       and t.txn_hour   = hc.txn_hour
),

-- ── Signal 3: Always-failed sender (≥5 txns, 100% failure) ───────────────────
sender_daily_stats as (
    select
        sender_vpa,
        txn_date,
        count(txn_id)                               as total,
        sum(case when status = 'FAILED' then 1 else 0 end) as failures
    from txns
    group by sender_vpa, txn_date
),

always_failed as (
    select sender_vpa, txn_date
    from sender_daily_stats
    where total >= 5 and failures = total
),

signal_3 as (
    select
        t.txn_id,
        t.sender_vpa,
        t.txn_date,
        t.amount_inr,
        t.txn_timestamp,
        'ALWAYS_FAILED_SENDER'                      as signal_type,
        0.70                                        as signal_score,
        '100% failure rate sender (≥5 daily txns)'  as signal_reason
    from txns t
    inner join always_failed af
        on t.sender_vpa = af.sender_vpa
       and t.txn_date   = af.txn_date
),

-- ── Signal 4: Suspicious reversal on high-value txn ──────────────────────────
signal_4 as (
    select
        txn_id,
        sender_vpa,
        txn_date,
        amount_inr,
        txn_timestamp,
        'HIGH_VALUE_REVERSAL'                       as signal_type,
        0.65                                        as signal_score,
        'Reversed transaction with amount > 50K'    as signal_reason
    from txns
    where status = 'REVERSED'
      and is_high_value
),

-- ── Signal 5: Cross-bank high-value transfer ──────────────────────────────────
signal_5 as (
    select
        txn_id,
        sender_vpa,
        txn_date,
        amount_inr,
        txn_timestamp,
        'CROSS_BANK_HIGH_VALUE'                     as signal_type,
        0.60                                        as signal_score,
        'High-value cross-bank transfer'            as signal_reason
    from txns
    where is_cross_bank
      and is_high_value
      and status = 'SUCCESS'
),

-- ── Union all signals ─────────────────────────────────────────────────────────
all_signals as (
    select * from signal_1
    union all select * from signal_2
    union all select * from signal_3
    union all select * from signal_4
    union all select * from signal_5
)

select
    s.*,
    -- Composite risk score (capped at 1.0)
    least(s.signal_score, 1.0)                      as capped_score,
    {{ audit_columns() }}
from all_signals s
