
with

customers as (select * from {{ ref('stg_customers') }})
,orders as (select * from {{ ref('fct_orders') }})

,customer_orders as (
    select 
    customer_id
    ,min(order_date) as first_order_date
    ,max(order_date) as most_recent_order_date
    ,count(order_id) as number_of_orders
    ,sum(payment_dollars) as lifetime_value
    from orders
    group by customer_id
)

,final as (
    select 
    c.customer_id
    , c.first_name
    , c.last_name
    , co.first_order_date
    , co.most_recent_order_date
    , coalesce(co.number_of_orders, 0) as number_of_orders
    , coalesce(lifetime_value, 0) as lifetime_value
    from customers c
    left join customer_orders co using (customer_id)
)

select * from final 
-- order by customer_id