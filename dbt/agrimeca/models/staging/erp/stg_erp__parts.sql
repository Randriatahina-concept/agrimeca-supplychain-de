with rankedparts as(
    select payload:id::string                  as part_id,
       payload:name::string                as parts_name,
       payload:category::string            as category_name,
       payload:uom::string                 as uom,
       payload:list_price::decimal(18,2)   as list_price,
       payload:deleted::boolean            as is_deleted,
       payload:audit.created_at::timestamp as created_at,
       payload:audit.updated_at::timestamp as updated_at,
         _run_id,
         _ingest_ts,
         row_number() over (
              partition by source_id
              order by source_updated_at desc, _ingest_ts desc) as version_rank
    from {{ source('bronze', 'erp_parts') }}
)
select * except(version_rank)
from rankedparts
where version_rank = 1 -- ne garder que la version la plus récente de chaque article   
        