{{
    config(
        materialized = 'incremental',
        unique_key   = 'txn_id',
        schema       = 'staging',
        tags         = ['staging', 'daily'],
        description  = 'Staging view: cleans and normalises raw UPI transaction data from Snowflake Raw',
    )
}}

/*
  stg_upi_transactions
  ─────────────────────
  Source  : Raw Snowflake table (loaded via COPY INTO)
  Purpose : Normalisation, deduplication, and derived columns.
            Downstream models consume from this view, never from Raw directly.

  Transformations applied:
    - Trim / upper whitespace on string columns
    - status vocabulary enforcement (fallback to UNKNOWN)
    - currency default to INR
    - txn_date coerced from txn_timestamp where missing
    - Null-safe boolean columns
*/

with source as (
    select * from {{ source('raw', 'raw_upi_transactions') }}
    {% if is_incremental() %}
    where _source_date >= (select coalesce(max(txn_date), '1970-01-01') from {{ this }})
    {% endif %}
),

deduped as (
    select
        *,
        row_number() over (partition by txn_id order by txn_timestamp desc) as _rn
    from source
),

cleaned as (
    select
        -- Identifiers
        trim(txn_id)                        as txn_id,
        trim(upi_ref)                       as upi_ref,

        -- VPA addresses
        lower(trim(sender_vpa))             as sender_vpa,
        lower(trim(receiver_vpa))           as receiver_vpa,

        -- Amount
        coalesce(amount, 0.00)              as amount_inr,
        coalesce(upper(trim(currency)), 'INR') as currency,

        -- Status — normalise to controlled vocabulary
        case upper(trim(status))
            when 'SUCCESS'   then 'SUCCESS'
            when 'COMPLETED' then 'SUCCESS'
            when 'FAILED'    then 'FAILED'
            when 'FAILURE'   then 'FAILED'
            when 'ERROR'     then 'FAILED'
            when 'PENDING'   then 'PENDING'
            when 'REVERSED'  then 'REVERSED'
            when 'REFUNDED'  then 'REVERSED'
            else 'UNKNOWN'
        end                                 as status,

        -- Timestamps
        txn_timestamp,
        coalesce(cast(txn_timestamp as date), _source_date) as txn_date,
        extract(hour from txn_timestamp)    as txn_hour,
        dayofweek(txn_timestamp)            as txn_dow,

        -- Derived boolean flags (null-safe)
        -- Snowflake dayofweek: 0=Sun, 6=Sat
        coalesce(dayofweek(txn_timestamp) in (0, 6), false) as is_weekend,
        coalesce(upi_ref is not null, true)        as is_upi,
        coalesce(amount > 50000, false)            as is_high_value,
        coalesce(extract(hour from txn_timestamp) not between 6 and 23, false) as is_off_hours,

        -- Bank extraction
        lower(trim(regexp_substr(sender_vpa, '[^@]+$'))) as sender_bank,
        lower(trim(regexp_substr(receiver_vpa, '[^@]+$'))) as receiver_bank,

        -- Device info
        upper(trim(device_type))            as device_type,
        trim(device_id)                     as device_id,
        trim(ip_address)                    as ip_address,
        initcap(trim(city))                 as city,
        trim(remarks)                       as remarks,

        -- Pipeline metadata
        ingested_at,
        _source,
        current_timestamp() as _processed_ts,

        -- Audit
        {{ audit_columns() }}

    from deduped
    where _rn = 1
      and txn_id is not null
      and txn_timestamp is not null
)

select * from cleaned
