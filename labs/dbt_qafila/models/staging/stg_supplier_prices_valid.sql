{{ config(materialized='view') }}

-- Valid supplier prices for downstream cost calculations.

select *
from {{ ref('stg_supplier_prices') }}
where unit_cost_sar > 0
  and isfinite(unit_cost_sar)