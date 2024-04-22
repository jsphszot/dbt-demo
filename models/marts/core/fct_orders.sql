with

orders as (select * from {{ ref('stg_orders') }})
,payments as (select * from {{ ref('stg_payments') }})

,final as (
    select
    *
    from orders o
    left join payments p using (order_id)
    where p.payment_status='success'
)

select * from final