with products as (
    select * from {{ source('mn_stores', 'product_details') }}
)
select * from products