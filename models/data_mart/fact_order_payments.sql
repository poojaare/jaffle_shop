with orders as (
     
        select  
                order_id,
                customer_id,
                order_date,
                status
        from {{ ref('stg_orders') }} 
),
payment as (
    
        select 
                payment_id, 
                order_id,
                payment_method, 
                amount
        from {{ ref('stg_payments') }}
),

fact_order as(

    select 
        orders.order_id,
        payment.payment_id,
        orders.customer_id,
        orders.status order_status,
        payment.payment_method,
        payment.amount as payment_amount,
        orders.order_date as order_date
    from orders 
    left join payment
    on orders.order_id = payment.order_id
)
select * from fact_order