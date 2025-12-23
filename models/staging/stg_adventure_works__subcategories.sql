with 
    source_subcategories as (
        select * 
        from {{ source('raw_adventure_works', 'production_productsubcategory') }}
    )

,   renamed as (
        select
            cast(productsubcategoryid as int) as subcategory_id
            , cast(name as string) as subcategory_name
            , cast(productcategoryid as int) as category_id
        from source_subcategories
    )

select * 
from renamed
