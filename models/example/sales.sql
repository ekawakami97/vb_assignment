/*
A transactional fact table for "sales",
with the grain set at the order item level
*/
with 
order_item_quantities as (
    select
        order_id,
        product_id,
        seller_id,
        price,
        freight_value,
        count(product_id) as order_item_quantity,
        sum(price) as total_order_item_value,
        sum(freight_value) as total_items_freight_value
    from raw.olist_order_items
    group by 1,2,3,4,5

),

final_select as (
    select
        orders.order_id,
        orders.customer_id,
        items_and_quantities.product_id,
        items_and_quantities.seller_id,
        items_and_quantities.price,
        items_and_quantities.freight_value,
        items_and_quantities.order_item_quantity,
        items_and_quantities.total_order_item_value,
        items_and_quantities.total_items_freight_value,
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date
    from raw.olist_orders as orders
    left join order_item_quantities as items_and_quantities
        on orders.order_id = items_and_quantities.order_id

)


select * from final_select