-- refund will never be greater than amount paid, so total amount needs to always be >= 0
-- return records where this isn't true to make test fail

with
payments as(
    select * from {{ ref('stg_payments') }}
)

select 
order_id
, sum(payment_dollars) as total_dollars
from payments
group by 1
having total_dollars<0