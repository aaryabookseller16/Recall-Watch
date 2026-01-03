with daily as (
    select received_date as d, count(*) as n
    from {{ ref('stg_complaints') }}
    where received_date is not null
    group by 1
),
x as (
    select
        d,
        n,
        lag(n) over (order by d) as prev_n
    from daily
)
select 1
from x
where prev_n is not null
  and n > 10 * prev_n