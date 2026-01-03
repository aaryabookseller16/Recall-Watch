with src as (
    select
        recall_pk,
        source,
        source_id,
        make,
        model,
        model_year,
        component,
        report_date,
        source_updated_at,
        ingested_at,
        raw_payload
    from {{ source('raw', 'recalls') }}
),

typed as (
    select
        recall_pk,
        source,
        source_id,
        nullif(trim(make), '') as make,
        nullif(trim(model), '') as model,
        model_year::int as model_year,
        nullif(trim(component), '') as component_raw,
        report_date::date as report_date,
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
                partition by recall_pk
                order by ingested_at desc
            ) as rn
        from normalized
    ) t
    where rn = 1
)

select
    recall_pk,
    source,
    source_id,
    make,
    model,
    model_year,
    component_raw,
    component_norm,
    report_date,
    ingested_at,
    raw_payload
from dedup