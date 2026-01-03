with src as (
    select
        complaint_pk,
        source,
        source_id,
        odi_number,
        make,
        model,
        model_year,
        component,
        incident_date,
        received_date,
        state,
        source_updated_at,
        ingested_at,
        raw_payload
    from {{ source('raw', 'complaints') }}
),

typed as (
    select
        complaint_pk,
        source,
        source_id,
        nullif(trim(odi_number), '') as odi_number,
        nullif(trim(make), '') as make,
        nullif(trim(model), '') as model,
        model_year::int as model_year,
        nullif(trim(component), '') as component_raw,
        incident_date::date as incident_date,
        received_date::date as received_date,
        nullif(trim(state), '') as state,
        ingested_at::timestamp as ingested_at,
        raw_payload
    from src
),

normalized as (
    select
        *,
        lower(regexp_replace(component_raw, '\s+', ' ', 'g')) as component_norm
    from typed
),

dedup as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by complaint_pk
                order by ingested_at desc
            ) as rn
        from normalized
    ) t
    where rn = 1
)

select
    complaint_pk,
    source,
    source_id,
    odi_number,
    make,
    model,
    model_year,
    component_raw,
    component_norm,
    incident_date,
    received_date,
    state,
    ingested_at,
    raw_payload
from dedup