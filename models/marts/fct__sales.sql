with 
    int_sales as (
        select *
        from {{ ref('int__sales') }}
    )

    , int_reasons as (
        select * 
        from {{ ref('int__reasons') }}
    )

    , joined_tables as (
        select 
            int_sales.*
            , int_reasons.reason_sk as sales_reason_fk
        from int_sales
        left join int_reasons
            on int_sales.order_id = int_reasons.order_id
    )

    , final as (
        select
            sales_sk
            , customer_fk
            , ship_date
            , order_date
            , due_date
            , product_fk
            , sales_reason_fk
            , ship_to_address_id
            , order_id
            , quantity
            , unit_price
            , discount
            , status
            , tax_amount * (unit_price * quantity * (1-discount)) 
                / sum (unit_price * quantity * (1-discount)) over (partition by order_id) as tax_allocated
            , freight_amount * (unit_price * quantity * (1-discount)) 
                / sum (unit_price * quantity * (1-discount)) over (partition by order_id) as freight_allocated
            , unit_price * quantity as gross_total
            , unit_price * quantity * (1-discount) as net_total
            , ordered_online
            , credit_card_id
            , credit_card_type
        from joined_tables
    )

select *
from final
