with 
    stg_products as (
        select * 
        from {{ ref('stg_adventure_works__products') }}
    )

    , stg_subcategories as (
        select *
        from {{ ref('stg_adventure_works__subcategories') }}
    )

    , stg_categories as (
        select *
        from {{ ref('stg_adventure_works__categories') }}
    )

    , joined_tables as (
        select 
            stg_products.product_id
            , stg_products.product_name
            , stg_products.product_number
            , stg_categories.category_name as product_category
            , stg_subcategories.subcategory_name as product_subcategory
            , stg_products.product_style
        from stg_products
        left join stg_subcategories on
            stg_products.subcategory_id = stg_subcategories.subcategory_id
        left join stg_categories on
            stg_subcategories.category_id = stg_categories.category_id
    )

select *
from joined_tables
