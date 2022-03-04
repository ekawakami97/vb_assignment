
with 
orders_combined as (
    select
        customers.customer_unique_id,
        customers.customer_id,
        sales.order_id,
        sales.order_purchase_timestamp,
        customers.customer_city,	
        customers.customer_state,
        round(sum(coalesce(sales.total_order_item_value,0)),2) as total_order_items_value,
        round(sum(coalesce(sales.total_items_freight_value,0)),2) as total_items_freight_value,
        round(sum(coalesce(sales.total_order_item_value,0))+ sum(coalesce(sales.total_items_freight_value,0)),2) as total_order_value
    from raw.olist_customers as customers
    inner join {{ ref('sales') }} as sales
    on sales.customer_id = customers.customer_id
    where sales.order_status <> 'canceled' and sales.order_status <> 'unavailable'
    group by 1,2,3,4,5,6
),

order_orders as (
    select
        *,
        row_number() over(partition by customer_unique_id order by order_purchase_timestamp) as orders_ascending,
        row_number() over(partition by customer_unique_id order by order_purchase_timestamp desc) as orders_descending
    from orders_combined
),

first_order as (
    select
        customer_unique_id,
        order_purchase_timestamp as first_order_timestamp
    from order_orders
    where orders_ascending = 1
),

/* 
Customer city/ state might change per order so just 
pull their location from their most recent order
*/
last_order as (
    select
        customer_unique_id,
        order_purchase_timestamp as most_recent_order_timestamp,
        customer_city,
        customer_state
    from order_orders
    where orders_descending = 1
),

total_orders as (
    select
        customer_unique_id,
        count(order_id) as total_orders
    from orders_combined
    group by 1

),

total_spent as (
    select  
        customer_unique_id,
        round(sum(total_order_value),2) as total_value_orders
    from orders_combined
    group by 1
),

final_select as (
    select
        distinct customers.customer_unique_id,
        first_order.first_order_timestamp,
        last_order.most_recent_order_timestamp,
        total_orders.total_orders,
        total_spent.total_value_orders,
        last_order.customer_city,
        last_order.customer_state
    from raw.olist_customers as customers
    left join first_order
        on customers.customer_unique_id = first_order.customer_unique_id
    left join last_order 
        on customers.customer_unique_id = last_order.customer_unique_id
    left join total_orders
        on customers.customer_unique_id = total_orders.customer_unique_id
    left join total_spent
        on customers.customer_unique_id = total_spent.customer_unique_id

)

select * from final_select