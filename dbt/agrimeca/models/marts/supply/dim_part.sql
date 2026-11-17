-- CTE 1 : base
with spend_by_part as (
select  part_id,
        sum(amount_eur) as spend_euro  -- on prend automatiquement le montant en EUR, car c'est la devise de reporting
from {{ ref('int_purchase_order_lines') }}
where is_valid 
group by part_id
),
-- CTE : Cumul des dépenses d'articles, du plus gros au plus petit, pour calculer le pourcentage de dépenses cumulées par article.
ranked as(
select part_id,spend_euro,
sum(spend_euro) over (
    order by spend_euro desc, part_id
    rows between unbounded preceding and current row) as cumulative_spend,  --  on peut aussi l'écrire : sum(spend_euro) over(order by spend_euro desc, part_id ) as cumulative_spend, -- part_id pour toutes les lignes qui ont la même valeur de tri ne reçoivent le même cumul
sum(spend_euro) over() as total_spend -- total des dépenses pour calculer le pourcentage de dépenses cumulées
from spend_by_part

),
-- CTE : classification des articles en catégories A, B et C selon la règle de Pareto (80/20).
classified as(
    select part_id,
        spend_euro,
        cumulative_spend,
        total_spend,
        cumulative_spend/total_spend as cumulative_share,
        case
            when cumulative_share <= 0.8 then 'A'
            when cumulative_share <= 0.95 then 'B'
            else 'C'
        end as abc_class
    from ranked
)
select
    abc_class,
    count(*)                          as nb_parts,
    round(sum(spend_euro) / 1e6, 1)   as spend_meur,
    round(max(cumulative_share), 3)   as max_share
from classified
group by abc_class
order by abc_class