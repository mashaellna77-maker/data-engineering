{{ config(materialized='table', schema='quarantine') }}

-- Grain: one rejected row from the staged supplier price feed.

select
    *,
    case
        when unit_cost_sar is null then 'missing_or_unparseable_cost'
        when not isfinite(unit_cost_sar) then 'non_finite_cost'
        when unit_cost_sar <= 0 then 'non_positive_cost'
    end as rejection_reason,
    'pending_review' as review_status
from {{ ref('stg_supplier_prices') }}
where unit_cost_sar is null
   or not isfinite(unit_cost_sar)
   or unit_cost_sar <= 0