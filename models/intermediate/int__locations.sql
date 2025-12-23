with 
    stg_shipped_to as (
        select *
        from {{ ref('stg_adventure_works__order_hearder') }}
    )

    , stg_address as (
        select * 
        from {{ ref('stg_adventure_works__address') }}
    )

    , stg_states_and_provinces as (
        select *
        from {{ ref('stg_adventure_works__states_and_provinces') }}
    )

    , stg_countries as (
        select *
        from {{ ref('stg_adventure_works__countries') }}
    )

    ,stg_territories as (
        select *
        from {{ ref('stg_adventure_works__territories') }}
    )

    , joined_tables as (
        select 
            distinct stg_shipped_to.ship_to_address_id as address_id
            , stg_address.city as city_name
            , stg_states_and_provinces.state_name
            , stg_countries.country_name as country_name
            , stg_territories.territory_continent as continent_name
        from stg_shipped_to
        left join stg_address on
            stg_shipped_to.ship_to_address_id = stg_address.address_id
        left join stg_states_and_provinces on
            stg_address.state_id = stg_states_and_provinces.state_id
        left join stg_countries on
            stg_states_and_provinces.country_code = stg_countries.country_code
        left join stg_territories on
            stg_states_and_provinces.territory_id = stg_territories.territory_id
    )

select *
from joined_tables