with 
    stg_customers as (
        select * 
        from {{ ref('stg_adventure_works__customers') }}
    )

    , stg_persons as (
        select *
        from {{ ref('stg_adventure_works__person') }}
    )

    , stg_stores as (
        select *
        from {{ ref('stg_adventure_works__stores') }}
    )

    , joined_tables as (
        select 
            stg_customers.customer_id
            , stg_customers.person_id
            , stg_customers.store_id
            , stg_persons.person_name as name
            , stg_stores.store_name
        from stg_customers
        left join stg_persons on 
            stg_customers.customer_id = stg_persons.business_entity_id
        left join stg_stores on
            stg_customers.store_id = stg_stores.businessentityid
    )

    , transformed_table as (
        select 
            customer_id
            , name
            , store_name
            , case 
                when person_id is not null and store_id is not null then 'Store Representative'
                when person_id is not null and store_id is null then 'Individual'
                when person_id is null and store_id is not null then 'Store'
            end as customer_type
        from joined_tables
    )

select *
from transformed_table
