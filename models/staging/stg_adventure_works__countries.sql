with 
    source_countries as (
        select * 
        from {{ source('raw_adventure_works', 'person_countryregion') }}
    )

,   renamed as (
        select
            cast(countryregioncode as string) as country_code
            , cast(name as string) as country_name
        from source_countries
    )

select * 
from renamed
