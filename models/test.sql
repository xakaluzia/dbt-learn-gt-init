with orders_payment as (
    select * from {{ref('stg_stripe__payment')}}
    where status = 'success'

),
amount_by_order_id as(
    select 
        order_id,
        sum(amount) as total_amount
    from 
        orders_payment 
    group by order_id 
    having total_amount < 0
)
select * from amount_by_order_id
