with 
    stg_reasons as (
        select * 
        from {{ ref('stg_adventure_works__sales_reasons') }}
    )

    , stg_order_reasons as (
        select *
        from {{ ref('stg_adventure_works__sales_order_reasons') }}
    )

    , joined_tables as (
        select
            stg_order_reasons.order_id
            , collect_set(stg_reasons.reason_name) as sales_reason
            , collect_set(stg_reasons.reason_type) as sales_reason_type
        from stg_order_reasons
        left join stg_reasons on 
            stg_order_reasons.reason_id = stg_reasons.reason_id
        group by 1
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id', 'sales_reason']) }} as reason_sk
            , *
        from joined_tables
    )

select *
from create_sk