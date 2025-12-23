with 
    source_address as (
        select * 
        from {{ source('raw_adventure_works', 'person_address') }}
    )

,   renamed as (
        select
            cast(addressid as int) as address_id
            , cast(city as string) as city
            , cast(stateprovinceid as int) as state_id 
        from source_address
    )

select * 
from renamed
