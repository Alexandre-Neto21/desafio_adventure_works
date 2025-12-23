with 
    source_categories as (
        select * 
        from {{ source('raw_adventure_works', 'production_productcategory') }}
    )

,   renamed as (
        select
            cast(productcategoryid as int) as category_id
            , cast(name as string) as category_name
        from source_categories
    )

select * 
from renamed
