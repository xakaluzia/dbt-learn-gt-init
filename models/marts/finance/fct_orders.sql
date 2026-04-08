with orders as(
    select * from {{ref('stg_jaffle_shop__orders')}}
),
payments as (
    select * from {{ref('stg_stripe__payment')}}
),
orders_payments as(
    select 
        order_id,
        coalesce(case when status = 'success' then amount end, 0) as amount, 
        created as payment_date
    from payments
   
),
final as (
    select 
        orders_payments.order_id,
        customer_id,
        orders_payments.amount,


    from orders
    left join orders_payments using(order_id)
)
select * from final