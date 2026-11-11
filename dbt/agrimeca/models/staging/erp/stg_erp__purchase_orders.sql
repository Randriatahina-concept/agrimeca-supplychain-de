-- Une commande par ligne : en-tête extrait du JSON, dates normalisées, dernière version retenue
with ranked as (
    select
        payload:id::string                         as po_id,
        payload:status::string                     as status,
        {{ parse_erp_date('payload:order_date') }} as order_date,   -- normalisation de la date, quel que soit le format d'origine avec le macro parse_erp_date
        payload:order_date::string                 as order_date_raw,      -- valeur d'origine, pour contrôle
        payload:currency::string                   as currency,            -- null sur ~2 % : imputée plus tard
        payload:plant.code::string                 as plant_code,
        payload:plant.name::string                 as plant_name,
        payload:supplier.id::string                as supplier_id,
        payload:deleted::boolean                   as is_deleted,
        payload:audit.created_at::timestamp        as created_at,
        payload:audit.updated_at::timestamp        as updated_at,
        _run_id,
        _ingest_ts,
        row_number() over (
            partition by source_id
            order by source_updated_at desc, _ingest_ts desc
        ) as version_rank                          -- 1 = la version la plus récente
    from {{ source('bronze', 'erp_purchase_orders') }}
)

select * except (version_rank)
from ranked
where version_rank = 1                             -- une ligne par commande