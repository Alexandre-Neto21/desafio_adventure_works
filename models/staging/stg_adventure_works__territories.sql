with 
    source_territory as (
        select *
        from {{ source('raw_adventure_works', 'sales_salesterritory') }}
    )

,   renamed as (
        select 
            cast(territoryid as int) as territory_id
            , cast(name as string) as territory_name
            , cast(group as string) as territory_continent
            , cast(salesytd as numeric(15,4)) as sales_current_year
            , cast(saleslastyear as numeric(15,4)) as sales_last_year
        from source_territory
    )

select *
from renamed
