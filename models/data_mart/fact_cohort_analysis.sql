with orders as (
     
        select  
                order_id,
                customer_id,
                order_date,
                status, 
                lead(order_date, 1) over (partition by customer_id order by order_date) as next_date,
                datediff(day,order_date, next_date) as day_diff_between_orders
        from {{ ref('stg_orders') }} 
),
cust as(

        select * from {{ ref('stg_customers') }}
),
payment as (
    
        select 
                payment_id, 
                order_id,
                payment_method, 
                amount
        from {{ ref('stg_payments') }}
),
cust_orders as(
    
        select 
            orders.customer_id as customer_id,
            min(order_date) first_order_date,
            max(order_date) last_order_date,
            count(order_id) as total_orders,
            avg(day_diff_between_orders) as average_days_between_orders
    from orders
    left join cust
    on orders.customer_id = cust.customer_id 
    group by orders.customer_id
),
cust_payment as(
    select
        orders.customer_id,
        coalesce(sum(amount),0) as revenue
    from payment
    left join orders on
         payment.order_id = orders.order_id
        where orders.status='completed'
    group by orders.customer_id
),
fact_cohort as(

    select 
            cust_orders.customer_id, 
            cust_orders.first_order_date,
            cust_orders.last_order_date,
            cust_orders.total_orders,
            coalesce(cust_payment.revenue,0) as total_revenue,
            coalesce(cust_orders.average_days_between_orders,0) as average_days_between_orders
    from cust_orders
    left join cust_payment
    on cust_orders.customer_id = cust_payment.customer_id
)
select * from fact_cohort order by customer_id

-- select * from cust/* customer_id, first_name, last_name */
-- select * from orders /* order_id, customer_id, order_date, status*/-- 