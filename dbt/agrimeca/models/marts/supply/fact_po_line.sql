-- fact_po_line : une ligne par ligne de commande valide (is_valid au Silver).
-- Clés vers dim_part, dim_supplier, dim_plant, dim_date ; mesures de quantités et de montants ;
-- indicateurs de livraison : OTIF (sur les lignes reçues), délais, retards en cours.
-- Les commandes annulées sont conservées (is_cancelled) : à exclure des mesures de dépenses.

with lines as (
    -- lignes valides du Silver : clés, dates, quantités, montants, statut
    select
        -- clés
        po_line_id,
        po_id,
        line_no,
        supplier_id,
        part_id,
        plant_code,
        -- dates
        order_date,
        promised_date,
        to_date(received_at)                                                  as received_date,
        -- quantités en unité de base (BOX12 converties en pièces)
        quantity_base,
        case when uom = 'BOX12' then received_qty * 12 else received_qty end  as received_qty_base,
        uom_base,
        -- montants
        unit_price,
        currency_final,
        fx_rate                                                               as fx_rate_per_eur,  -- unités de devise pour 1 EUR
        amount_eur,
        -- statut de la commande
        po_status,
        is_deleted
    from {{ ref('int_purchase_order_lines') }}
    where is_valid
),

measured as (
    -- indicateurs de livraison ; null = pas encore mesurable (ligne non reçue)
    select
        *,
        po_status = 'CANCELLED'                      as is_cancelled,
        received_date is not null                    as is_received,
        received_date <= promised_date               as is_on_time,
        received_qty_base >= quantity_base           as is_in_full,
        is_on_time and is_in_full                    as is_otif,
        datediff(day, order_date, received_date)     as lead_time_days,
        datediff(day, promised_date, received_date)  as delay_days,       -- > 0 = retard
        promised_date < current_date()
            and received_date is null
            and not is_cancelled
            and not coalesce(is_deleted, false)      as is_open_late
    from lines
)

select
    -- clés
    po_line_id,
    po_id,
    line_no,
    supplier_id,
    part_id,
    plant_code,
    -- dates
    order_date,
    promised_date,
    received_date,
    -- quantités
    quantity_base,
    received_qty_base,
    uom_base,
    -- montants
    unit_price,
    currency_final,
    fx_rate_per_eur,
    amount_eur,
    -- statut
    po_status,
    is_cancelled,
    -- indicateurs de livraison
    is_received,
    is_on_time,
    is_in_full,
    is_otif,
    lead_time_days,
    delay_days,
    is_open_late
from measured