with 
    source_products as (
        select * 
        from {{ source('raw_adventure_works', 'production_product') }}
    )

,   renamed as (
        select
            cast(productid as int) as product_id
            , cast(name as string) as product_name
            , cast(productnumber as string) as product_number
            , cast(style as string) as product_style
            , cast(productsubcategoryid as int) as subcategory_id
        from source_products
    )

select * 
from renamed
