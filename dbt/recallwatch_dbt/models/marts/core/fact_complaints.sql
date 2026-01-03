select
    c.complaint_pk,
    md5(coalesce(c.make,'') || '|' || coalesce(c.model,'') || '|' || coalesce(c.model_year::text,'')) as vehicle_id,
    md5(c.component_norm) as component_id,
    c.received_date as date_id,
    c.odi_number,
    c.state,
    c.source,
    c.source_id,
    c.component_raw,
    c.component_norm,
    c.ingested_at
from {{ ref('stg_complaints') }} c
where c.received_date is not null