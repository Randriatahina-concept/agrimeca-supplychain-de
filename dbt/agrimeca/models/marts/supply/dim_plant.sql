-- Créer une table de dimension pour les usines (plants) à partir des données de la table de staging des commandes d'achat (purchase_orders).
-- Chaque usine est identifiée par son code et son nom, extraits de l'en-t
with plant_ranked as (
select plant_code, plant_name,order_date,
    row_number() over (
        partition by plant_code
        order by  order_date desc
    ) as version_rank
from {{ ref('stg_erp__purchase_orders') }}
where plant_code is not null --- Exclure les lignes sans code d'usine, car elles ne peuvent pas être utilisées pour créer une dimension d'usine
)
select plant_code, plant_name
from plant_ranked
where version_rank = 1 -- ne garder que la version la plus récente de chaque usine
