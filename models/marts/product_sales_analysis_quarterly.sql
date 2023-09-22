WITH sales_with_year_quarter AS (
    SELECT
        product,
        financial_quarter,
        ordered_year,
        country,
        quantity
    FROM
         {{ ref('int_retailers_and_daily_sales') }}
    WHERE
        EXTRACT(YEAR FROM Date) IN (2015, 2016, 2017)
),

-- Final query to calculate sales by year and quarter
fin_qu AS (
    SELECT
    product,
    ordered_year,
    financial_quarter,
    country,
    SUM(quantity) AS total_quantity_sold
FROM
    sales_with_year_quarter
GROUP BY 
    product,ordered_year,financial_quarter, country    
ORDER BY
    product, ordered_year, financial_quarter, country
)

SELECT * FROM fin_qu
