with 
    fct_sales as (
        select *
        from {{ ref('fct__sales') }}
    )

    , gross_sales_2011 as(
        select
            sum(gross_total) as total_gross_sales
        from fct_sales
        where order_date between '2011-01-01' and '2011-12-31'
    )

select *
from gross_sales_2011
where abs(total_gross_sales - 12646112.16) >= 0.009
