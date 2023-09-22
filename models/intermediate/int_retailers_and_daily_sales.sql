with
    retailers as (
        select
            retailer_code, retailer_name as company_name, type as company_type, country
        from {{ ref("stg_retailers") }}
    ),

    date_quarters as (
    select
        Date,
        case
            when extract(month from Date) between 1 and 3 then 'Q1'
            when extract(month from Date) between 4 and 6 then 'Q2'
            when extract(month from Date) between 7 and 9 then 'Q3'
            else 'Q4'
        end as financial_quarter
    from
        {{ ref("stg_daily_sales") }}
    ),


    daily_sales as (
            select
                    retailer_code,
                    product_number,
                    order_method_code,
                    Date,
                    FORMAT_DATE('%Y-%b-%d', Date) as formatted_Date,
                    extract(year from Date) as ordered_year,
                    financial_quarter,
                    quantity,
                    selling_price,
                    cost_price,
                    round((selling_price - cost_price) * quantity, 0) as profit
            from
                    {{ ref("stg_daily_sales") }} 
            inner join
                    date_quarters
            USING(Date)
    ),

    products AS (
        SELECT * FROM {{ ref('stg_products') }}
    ),

    order_type AS (
        SELECT * FROM {{ ref('stg_order_methods') }}
    ),

    retailers_daily_sales as (
        select * from retailers inner join daily_sales using (retailer_code)
    ),

    ret_das_pr AS (
        select * 
        from retailers_daily_sales
        INNER JOIN
        products
        USING(PRODUCT_NUMBER)
    ),

    ret_das_pr_od AS (
        SELECT * 
        FROM ret_das_pr
        INNER JOIN
        order_type
        USING(ORDER_METHOD_CODE)
    )

select * from ret_das_pr_od