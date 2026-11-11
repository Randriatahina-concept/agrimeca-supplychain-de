with latest_orders as (
select 
        source_id, -- identifiant de la commande, unique dans le système ERP
        payload,
        _run_id,
        _ingest_ts,
        row_number() over(
            partition by source_id -- recommence à 1 pour chaque commande
            order by source_updated_at desc, _ingest_ts desc
        ) as version_rank -- 1 = la version la plus récente
from {{ source('bronze', 'erp_purchase_orders') }}
),

exploded as (
select source_id as po_id,
explode(from_json(payload:lines,'array<string>')) as line , -- une ligne de sortie par élément
_run_id,
_ingest_ts
from latest_orders
where version_rank = 1 -- ne garder que la version la plus récente de chaque commande
)
select
    po_id || '-' || line:line_no::string           as po_line_id,       -- clé unique de la ligne
    po_id,
    line:line_no::int                              as line_no,
    line:part.id::string                           as part_id,
    line:quantity.value::decimal(18, 3)            as quantity,
    line:quantity.uom::string                      as uom,
    line:unit_price.amount::decimal(18, 2)         as unit_price,
    line:unit_price.currency::string               as currency,
    {{ parse_erp_date('line:promised_date') }}     as promised_date,
    line:promised_date::string                     as promised_date_raw,
    line:receipt.date::timestamp                   as received_at,      -- null tant que non reçue
    line:receipt.quantity::decimal(18, 3)          as received_qty,
    _run_id,
    _ingest_ts
from exploded