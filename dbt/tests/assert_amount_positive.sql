-- Tests that all transactions have a positive amount
select *
from {{ ref('stg_upi_transactions') }}
where amount_inr < 0
