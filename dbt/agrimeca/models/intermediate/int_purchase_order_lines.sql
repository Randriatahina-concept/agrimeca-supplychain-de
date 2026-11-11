
--- Enrichir les lignes de commande avec les informations de la table des commandes d'achat
-- CTE 1 :Récupérer toutes les lignes de commande (stg_erp__purchase_order_lines)
with lines as(
select * from {{ ref('stg_erp__purchase_order_lines') }} 
),

-- CTE 2: Récupérer les informations de la table des commandes d'achat (en-tête de commande)
orders as(
select po_id,order_date,supplier_id,plant_code,status,is_deleted
from {{ ref('stg_erp__purchase_orders') }}
),

-- CTE 3: Récupérer les informations de la table des fournisseurs (stg_erp__suppliers)
suppliers as (
    select supplier_id, default_currency
    from {{ ref('stg_erp__suppliers') }}
),

-- CTE 4 : Récupérer les taux de change BCE quotidiens : un taux par devise et par jour calendaire
-- (week-ends et fériés complétés par le dernier taux publié, EUR = 1).
-- Sert à convertir le montant de chaque ligne en EUR, au taux du jour de commande.
fx as (
    select
        currency,       -- devise (ISO 4217), clé de jointure avec currency_final
        rate_date,      -- jour du taux, clé de jointure avec order_date
        rate            -- unités de devise pour 1 EUR : montant_eur = montant / rate
    from {{ ref('int_fx_rates_daily') }}
),
-- CTE 4: joindre les lignes de commande avec les informations de l'en-tête de commande
joined as (
    select
        l.po_line_id,
        l.po_id,
        l.line_no,
        o.order_date,                 -- de l'en-tête
        o.supplier_id,                -- de l'en-tête
        o.plant_code,                 -- de l'en-tête
        o.status      as po_status,   -- de l'en-tête
        o.is_deleted,                 -- de l'en-tête
        l.part_id,
        l.quantity,
        l.uom,
        l.unit_price,
        l.currency,
        l.promised_date,
        l.received_at,
        l.received_qty
    from lines as l
    left join orders as o
        on o.po_id = l.po_id
),

-- CTE 5: imputer la devise de la ligne de commande si elle est manquante
imputed as (
    select
        j.*,
        coalesce(j.currency, s.default_currency)    as currency_final,     -- devise de la ligne, sinon celle du fournisseur
        j.currency is null                          as is_currency_imputed  -- garder is_currency_imputed pour auditer et montrer que la devise a été imputée
    from joined as j
    left join suppliers as s
        on s.supplier_id = j.supplier_id
),
converted as (
    select
        i.*,
        case when i.uom = 'BOX12' then i.quantity * 12 else i.quantity end   as quantity_base, --case when ramènent les quantités commandées en boîtes à un nombre de pièces (10 boîtes de 12 vis, soit 120 vis)
        case when i.uom = 'BOX12' then 'PCE' else i.uom end                  as uom_base,  --Si la quantité a été transformée en pièces, son unité doit le dire aussi
        i.quantity * i.unit_price                                            as amount,         -- dans la devise de la ligne
        f.rate                                                               as fx_rate,
        round(i.quantity * i.unit_price / f.rate, 2)                         as amount_eur -- convertit le montant de la ligne en EUR, au taux du jour de commande
    from imputed as i
    left join fx as f
        on  f.currency  = i.currency_final      -- la devise après imputation
        and f.rate_date = i.order_date          -- le taux du jour de la commande
),
-- -CTE 8 : contrôle qualité : nommer le défaut de chaque ligne (null = ligne saine)
checked as (
    select
        c.*,
        case
            when c.order_date is null    then 'ORDER_HEADER_MISSING'   -- ligne sans en-tête
            when c.quantity <= 0         then 'QTY_NEGATIVE_OR_ZERO'   -- quantité impossible
            when c.quantity >= 1000      then 'QTY_OUTLIER'            -- quantité ×1 000
            when c.fx_rate is null       then 'FX_RATE_MISSING'        -- conversion en EUR impossible
        end as dq_issue
    from converted as c
)

select
    *,
    dq_issue is null as is_valid           -- vrai : la ligne peut alimenter le Gold
from checked
