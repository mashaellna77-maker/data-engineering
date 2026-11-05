{{ config(severity='error') }}

select
    f.order_line_id
from {{ ref('fct_order_items') }} f
join {{ ref('qrn_order_items') }} q
    on f.order_line_id = q.order_line_id