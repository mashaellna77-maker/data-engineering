{{ config(severity='error') }}

select *
from {{ ref('stg_supplier_prices_valid') }}
where unit_cost_sar is null
   or not isfinite(unit_cost_sar)
   or unit_cost_sar <= 0