/*A dimension table for products, 
which should include the following dimensionalized facts for each product:
total units sold
total revenue*/

with 
product_totals as (
    select
        product_id,
        sum(order_item_quantity) as total_units_sold,
        round((sum(total_order_item_value)+sum(total_items_freight_value)),2) as total_revenue
    from {{ ref('sales') }}
    where sales.order_status <> 'canceled' 
        and sales.order_status <> 'unavailable'
    group by 1
),

products_combined as (
    select
        products.*,
        coalesce(product_totals.total_units_sold,0) as total_units_sold,
        coalesce(product_totals.total_revenue,0) as total_revenue
    from raw.olist_products as products
    left join product_totals
        on products.product_id = product_totals.product_id
),

final_select as (
    select
        products.product_id,
        translation.string_field_1 as product_category,
        products.product_name_lenght as product_name_length,
        products.product_description_lenght as product_description_length,
        products.product_photos_qty,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,
        products.total_units_sold,
        products.total_revenue
    from products_combined as products
    left join raw.product_category_name_translation as translation
    on products.product_category_name = translation.string_field_0

)

select * from final_select

