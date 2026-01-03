select
    r.recall_pk,
    md5(coalesce(r.make,'') || '|' || coalesce(r.model,'') || '|' || coalesce(r.model_year::text,'')) as vehicle_id,
    md5(r.component_norm) as component_id,
    r.report_date as date_id,
    r.source,
    r.source_id,
    r.component_raw,
    r.component_norm,
    r.ingested_at
from {{ ref('stg_recalls') }} r
where r.report_date is not null