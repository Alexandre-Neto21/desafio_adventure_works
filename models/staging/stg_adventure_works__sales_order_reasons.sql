with 
    source_sales_order_reason as (
        select * 
        from {{ source('raw_adventure_works', 'sales_salesorderheadersalesreason') }}
    )

,   renamed as (
        select
            cast(salesorderid as int) as order_id
            , cast(salesreasonid as int) as reason_id
        from source_sales_order_reason
    )

select * 
from renamed
