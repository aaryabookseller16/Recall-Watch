with components as (
    select component_norm as component
    from {{ ref('stg_recalls') }}
    where component_norm is not null

    union

    select component_norm as component
    from {{ ref('stg_complaints') }}
    where component_norm is not null
)

select
    md5(component) as component_id,
    component as component_name
from components
group by 1,2