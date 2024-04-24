with
-- missing a way to determine what payment to consider for each order 
-- multiple successful payments exist for same order_id, from different methods and of different amounts
payments as (
    select 
    id as payment_id
    , orderid as order_id
    , paymentmethod as payment_method
    , status as payment_status 
    , amount/100 as payment_dollars
    -- , row_number() over (partition by orderid order by created desc)
    -- , row_number() over (partition by orderid order by id desc)
    , created as payment_at
    from {{ source('stripe', 'payment') }}
    order by orderid
)
select * from payments