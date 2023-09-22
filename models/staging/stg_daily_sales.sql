with daily_sales as (
    select *
    from {{ source('mn_stores', 'daily_sales') }}
)
select * from daily_sales