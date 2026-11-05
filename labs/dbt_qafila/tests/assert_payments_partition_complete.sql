{{ config(severity='error') }}

with routed as (
    select payment_id
    from {{ ref('stg_payments') }}

    union all

    select payment_id
    from {{ ref('qrn_payments') }}
),

missing_rows as (
    select payment_id
    from {{ source('raw', 'payments') }}

    except all

    select payment_id
    from routed
),

extra_rows as (
    select payment_id
    from routed

    except all

    select payment_id
    from {{ source('raw', 'payments') }}
)

select 'missing' as issue, payment_id
from missing_rows

union all

select 'extra' as issue, payment_id
from extra_rows