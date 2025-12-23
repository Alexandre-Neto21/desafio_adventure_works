with 
    stg_order_headers as (
        select *
        from {{ ref('stg_adventure_works__order_hearder') }}
    )

    , stg_order_details as (
        select * 
        from {{ ref('stg_adventure_works__order_details') }}
    )

    ,stg_credit_cards as (
        select *
        from {{ ref('stg_adventure_works__credit_cards') }}
    )

    , joined_tables as (
        select 
            stg_order_headers.customer_id as customer_fk
            , stg_order_headers.ship_date
            , stg_order_headers.order_date
            , stg_order_headers.due_date
            , stg_order_details.product_id as product_fk
            , stg_order_headers.ship_to_address_id
            , stg_order_headers.order_id 
            , stg_order_details.quantity
            , stg_order_details.unit_price
            , stg_order_details.discount
            , stg_order_headers.order_status as status 
            , stg_order_headers.tax_amount
            , stg_order_headers.freight as freight_amount
            , stg_order_headers.subtotal
            , stg_order_headers.ordered_online
            , stg_order_headers.credit_card_id
            , stg_credit_cards.credit_card_type
        from stg_order_details
        left join stg_order_headers on
            stg_order_details.order_id = stg_order_headers.order_id
        left join stg_credit_cards on
            stg_order_headers.credit_card_id = stg_credit_cards.credit_card_id 
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id', 'product_fk']) }} as sales_sk
            , *
        from joined_tables
    )

select *
from create_sk
