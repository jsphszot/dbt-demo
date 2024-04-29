with
payments as(
    select * from {{ ref('stg_payments') }}
)

select 
payment_id
, payment_dollars
from payments
where payment_dollars<0