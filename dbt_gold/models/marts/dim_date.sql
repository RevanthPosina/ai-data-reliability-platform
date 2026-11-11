{# Calendar table. Negative test (fixture): var contract_break_test makes date_key a varchar, which the enforced
   contract must reject. #}
with days as (
    select d as calendar_date
    from unnest(sequence(
        date '{{ var("dim_date_start", "2025-12-01") }}',
        date '{{ var("dim_date_end", "2027-12-31") }}'
    )) as t(d)
)

select
    {% if var('contract_break_test', false) -%}
    cast({{ date_key('calendar_date') }} as varchar) as date_key,
    {%- else -%}
    {{ date_key('calendar_date') }} as date_key,
    {%- endif %}
    calendar_date,
    cast(year(calendar_date) as integer) as year,
    cast(month(calendar_date) as integer) as month,
    cast(day(calendar_date) as integer) as day,
    cast(week(calendar_date) as integer) as iso_week,
    cast(day_of_week(calendar_date) as integer) as day_of_week
from days
