with 
    source_person as (
        select * 
        from {{ source('raw_adventure_works', 'person_person') }}
    )

,   renamed as (
        select
            cast(businessentityid as int) as business_entity_id
            , cast(persontype as string) as person_type
            , cast(firstname as string) || ' ' || cast(lastname as string) as person_name

        from source_person
    )

select * 
from renamed
