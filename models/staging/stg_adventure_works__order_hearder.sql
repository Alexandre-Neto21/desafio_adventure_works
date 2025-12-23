with 
    source_order_header as (
        select * 
        from {{ source('raw_adventure_works', 'sales_salesorderheader') }}
    )

,   renamed as (
        select
            cast(salesorderid as int) as order_id
            , cast(orderdate as date) as order_date
            , cast(duedate as date) as due_date
            , cast(shipdate as date) as ship_date
            , cast(status as string) as order_status
            , case
                when onlineorderflag = 'true' then "Yes"
                when onlineorderflag = 'false' then "No"
            end as ordered_online
            , cast(customerid as int) as customer_id
            , cast(salespersonid as int) as sales_person_id
            , cast(territoryid as int) as territory_id
            , cast(billtoaddressid as int) as bill_to_address_id
            , cast(shiptoaddressid as int) as ship_to_address_id
            , cast(creditcardid as int) as credit_card_id
            , cast(subtotal as numeric(15,4)) as subtotal
            , cast(taxamt as numeric(15,4)) as tax_amount
            , cast(freight as numeric(15,4)) as freight
            , cast(totaldue as numeric(15,4)) as total
        from source_order_header
    )

select * 
from renamed
