{{ config(materialized='table', schema='quarantine') }}

-- Grain: one row per rejected payment attempt.

select
    payment_id,
    order_id,
    amount,
    currency,
    method,
    wallet_provider,
    status,
    card_last4,
    created_at,
    updated_at,

    case
        when amount is null then 'missing_amount'
        when not isfinite(amount) then 'non_finite_amount'
        when amount <= 0 then 'non_positive_amount'
    end as rejection_reason,

    'pending_review' as review_status

from {{ source('raw', 'payments') }}

where amount is null
   or not isfinite(amount)
   or amount <= 0