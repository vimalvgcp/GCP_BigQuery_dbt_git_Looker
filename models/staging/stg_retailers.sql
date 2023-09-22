with retailers as (
    select * from {{ source('mn_stores', 'retailers_details') }}
)
select * from retailers