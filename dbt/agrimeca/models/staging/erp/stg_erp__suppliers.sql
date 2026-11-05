-- Un fournisseur par ligne : champs extraits du JSON, typés, dernière version retenue
with ranked as (
    select
        payload:id::string                   as supplier_id,
        payload:name::string                 as supplier_name,
        payload:country::string              as country_code,
        payload:default_currency::string     as default_currency,
        payload:deleted::boolean             as is_deleted,
        payload:audit.created_at::timestamp  as created_at,
        payload:audit.updated_at::timestamp  as updated_at,
        _run_id,
        _ingest_ts,
        row_number() over (
            partition by source_id
            order by source_updated_at desc, _ingest_ts desc
        ) as version_rank                    -- 1 = la version la plus récente
    from {{ source('bronze', 'erp_suppliers') }}
)

select * except (version_rank)
from ranked
where version_rank = 1