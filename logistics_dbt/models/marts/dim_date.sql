-- Date spine covering a wide range around any realistic shipment date.
-- Regenerate/extend the range if the data ever needs dates outside it.
with date_spine as (
    select
        dateadd(day, seq4(), '2023-01-01'::date) as date_day
    from table(generator(rowcount => 1826))  -- 2023-01-01 .. 2027-12-31
)

select
    date_day as date_key,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    dayname(date_day) as day_of_week,
    quarter(date_day) as quarter
from date_spine
