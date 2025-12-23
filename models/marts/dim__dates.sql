with 
    date_spine as (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2011-05-31' as date)",
            end_date="cast('2014-06-30' as date)"
            )
        }}
    )

    , complete_calendar as (
        select
        cast (date_day as date) as date
        , extract(year from date_day) as year
        , extract(month from date_day) as month
        , extract(day from date_day) as day
        , extract(quarter from date_day) as quarter
        , case 
                when month = 1 then 'Janeiro'
                when month = 2 then 'Fevereiro'
                when month = 3 then 'Março'
                when month = 4 then 'Abril'
                when month = 5 then 'Maio'
                when month = 6 then 'Junho'
                when month = 7 then 'Julho'
                when month = 8 then 'Agosto'
                when month = 9 then 'Setembro'
                when month = 10 then 'Outubro'
                when month = 11 then 'Novembro'
                when month = 12 then 'Dezembro'
            end as month_name
        , month || '/' || year as month_year
        , month_name || '/' || year as month_name_year
        , quarter || '/' || year as quarter_year
    from date_spine
    )

select *
from complete_calendar
