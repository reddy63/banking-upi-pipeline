{{
    config(
        materialized = 'table',
        schema       = 'mart_fraud',
        tags         = ['mart', 'fraud', 'daily'],
        description  = 'Composite fraud score per transaction based on triggered signals',
        unique_key   = 'txn_id',
    )
}}

with signals as (
    select * from {{ ref('fct_fraud_signals') }}
),

aggregated_scores as (
    select
        txn_id,
        sender_vpa,
        txn_timestamp,
        sum(signal_score) as total_raw_score,
        -- Cap the total score at 1.0 (100% risk)
        least(sum(signal_score), 1.0) as fraud_score,
        count(signal_type) as signals_triggered,
        array_agg(signal_type) as triggered_rules
    from signals
    group by txn_id, sender_vpa, txn_timestamp
)

select
    *,
    case
        when fraud_score >= 0.8 then 'CRITICAL'
        when fraud_score >= 0.5 then 'HIGH'
        when fraud_score >= 0.2 then 'MEDIUM'
        else 'LOW'
    end as fraud_risk_tier,
    {{ audit_columns() }}
from aggregated_scores
