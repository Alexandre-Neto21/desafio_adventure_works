with 
    source_order_details as (
        select * 
        from {{ source('raw_adventure_works', 'sales_salesorderdetail') }}
    )

,   renamed as (
        select
            cast(salesorderdetailid as int) as order_detail_id
            , cast(salesorderid as int) as order_id
            , cast(orderqty as int) as quantity
            , cast(productid as int) as product_id
            , cast(unitprice as numeric(10,2)) as unit_price
            , cast(unitpricediscount as numeric(10,2)) as discount
        from source_order_details
    )

select * 
from renamed
