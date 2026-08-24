-- Tests that no transaction is recorded in the future
select *
from {{ ref('stg_upi_transactions') }}
where txn_timestamp > current_timestamp()
