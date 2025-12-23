with 
    source_states_and_provinces as (
        select * 
        from {{ source('raw_adventure_works', 'person_stateprovince') }}
    )

,   renamed as (
        select
            cast(stateprovinceid as int) as state_id
            , cast(countryregioncode as string) as country_code
            , cast(name as string) as state_name
            , cast(territoryid as int) as territory_id
        from source_states_and_provinces
    )

select * 
from renamed
