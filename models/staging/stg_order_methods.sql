with order_methods as (
    select * from {{ source('mn_stores', 'order_methods') }}
)
select * from order_methods

