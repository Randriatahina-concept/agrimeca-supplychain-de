-- dim_part : une ligne par article, avec ses attributs et sa classe ABC.
-- Articles = référentiel (stg_erp__parts) ∪ articles commandés (int_purchase_order_lines) :
--   * articles commandés mais absents du référentiel → 'Article inconnu', is_unknown_part = true
--   * articles sans dépense (jamais commandés, ou seulement
--     dans des commandes annulées)                    → spend_euro = 0, abc_class = 'N'
-- Dépense = lignes valides, hors commandes annulées, en EUR.
-- Classe ABC : A jusqu'à 80 % des dépenses cumulées, B jusqu'à 95 %, C au-delà.

with spend_by_part as (
    -- un article par part_id commandé (lignes valides) ;
    -- les commandes annulées comptent pour 0 € mais l'article reste présent,
    -- pour que toute clé de fact_po_line existe dans la dimension
    select
        part_id,
        sum(case when po_status <> 'CANCELLED' then amount_eur else 0 end) as spend_euro
    from {{ ref('int_purchase_order_lines') }}
    where is_valid
    group by part_id
),

ranked as (
    -- cumul des dépenses du plus gros article au plus petit ;
    -- part_id départage les ex-æquo pour un ordre déterministe
    select
        part_id,
        spend_euro,
        sum(spend_euro) over (
            order by spend_euro desc, part_id
            rows between unbounded preceding and current row
        )                         as cumulative_spend,
        sum(spend_euro) over ()   as total_spend
    from spend_by_part
),

classified as (
    -- part cumulée et classe ABC ; N si aucune dépense
    select
        part_id,
        spend_euro,
        case
            when spend_euro = 0 then null
            else cumulative_spend / total_spend
        end as cumulative_share,
        case
            when spend_euro = 0              then 'N'
            when cumulative_share <= 0.80    then 'A'
            when cumulative_share <= 0.95    then 'B'
            else 'C'
        end as abc_class
    from ranked
),

parts as (
    -- référentiel des articles
    select
        part_id,
        parts_name,
        category_name,
        uom,
        list_price
    from {{ ref('stg_erp__parts') }}
)

select
    coalesce(p.part_id, c.part_id)            as part_id,
    coalesce(p.parts_name, 'Article inconnu') as part_name,
    p.category_name                           as category,
    p.uom                                     as uom,
    p.list_price                              as list_price,
    p.part_id is null                         as is_unknown_part,   -- commandé mais absent du référentiel
    coalesce(c.spend_euro, 0)                 as spend_euro,        -- hors commandes annulées
    c.cumulative_share,                                             -- null si aucune dépense
    coalesce(c.abc_class, 'N')                as abc_class          -- N = aucune dépense
from parts as p
full outer join classified as c
    on c.part_id = p.part_id