with 
    source_stores as (
        select * 
        from {{ source('raw_adventure_works', 'sales_store') }}
    )

,   renamed as (
        select
            cast(businessentityid as int)
            , cast(name as string) as store_name
        from source_stores
    )

select * 
from renamed
