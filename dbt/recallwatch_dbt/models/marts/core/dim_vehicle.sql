with vehicles as (
    select make, model, model_year
    from {{ ref('stg_complaints') }}
    where make is not null

    union

    select make, model, model_year
    from {{ ref('stg_recalls') }}
    where make is not null
)

select
    md5(coalesce(make,'') || '|' || coalesce(model,'') || '|' || coalesce(model_year::text,'')) as vehicle_id,
    make,
    model,
    model_year
from vehicles
group by 1,2,3,4