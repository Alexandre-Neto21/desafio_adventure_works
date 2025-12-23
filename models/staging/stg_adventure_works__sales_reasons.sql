with 
    source_sales_reason as (
        select * 
        from {{ source('raw_adventure_works', 'sales_salesreason') }}
    )

,   renamed as (
        select
            cast(salesreasonid as int) as reason_id
            , cast(name as string) as reason_name
            , cast(reasontype as string) as reason_type
        from source_sales_reason
    )

select * 
from renamed
