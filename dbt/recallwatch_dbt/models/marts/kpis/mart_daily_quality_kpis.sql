with daily_recalls as (
    select
        date_id,
        vehicle_id,
        component_id,
        count(*) as recall_count
    from {{ ref('fact_recalls') }}
    group by 1,2,3
),

daily_complaints as (
    select
        date_id,
        vehicle_id,
        component_id,
        count(*) as complaint_count
    from {{ ref('fact_complaints') }}
    group by 1,2,3
),

joined as (
    select
        coalesce(c.date_id, r.date_id) as date_id,
        coalesce(c.vehicle_id, r.vehicle_id) as vehicle_id,
        coalesce(c.component_id, r.component_id) as component_id,
        coalesce(r.recall_count, 0) as recall_count,
        coalesce(c.complaint_count, 0) as complaint_count
    from daily_complaints c
    full outer join daily_recalls r
      on c.date_id = r.date_id
     and c.vehicle_id = r.vehicle_id
     and c.component_id = r.component_id
),

rollups as (
    select
        date_id,
        vehicle_id,
        component_id,
        recall_count,
        complaint_count,

        sum(complaint_count) over (
            partition by vehicle_id, component_id
            order by date_id
            rows between 6 preceding and current row
        ) as complaints_7d,

        sum(complaint_count) over (
            partition by vehicle_id, component_id
            order by date_id
            rows between 29 preceding and current row
        ) as complaints_30d
    from joined
),

growth as (
    select
        *,
        (complaints_7d - lag(complaints_7d, 7) over (partition by vehicle_id, component_id order by date_id))
            as complaints_7d_growth
    from rollups
)

select
    g.date_id,
    g.vehicle_id,
    v.make,
    v.model,
    v.model_year,
    g.component_id,
    comp.component_name as component,
    g.recall_count,
    g.complaint_count,
    g.complaints_7d,
    g.complaints_30d,
    g.complaints_7d_growth
from growth g
left join {{ ref('dim_vehicle') }} v on g.vehicle_id = v.vehicle_id
left join {{ ref('dim_component') }} comp on g.component_id = comp.component_id