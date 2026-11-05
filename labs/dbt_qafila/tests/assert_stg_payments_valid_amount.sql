{{ config(severity='error') }}

select
    payment_id,
    amount
from {{ ref('stg_payments') }}
where amount is null
   or not isfinite(amount)
   or amount <= 0