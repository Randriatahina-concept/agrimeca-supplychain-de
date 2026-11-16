-- Dimension des fournisseurs.
-- Grain : une ligne par fournisseur (supplier_id), déjà garanti par stg_erp__suppliers.
-- Ajoute le nom du pays et la région d'achat. Les fournisseurs supprimés restent présents :
-- leurs anciennes commandes doivent toujours retrouver leur fournisseur.
select
    supplier_id,
    supplier_name,
    country_code,
    default_currency,
    is_deleted,

    case country_code
        when 'FR' then 'France'
        when 'DE' then 'Allemagne'
        when 'IT' then 'Italie'
        when 'ES' then 'Espagne'
        when 'PL' then 'Pologne'
        when 'GB' then 'Royaume-Uni'
        when 'TR' then 'Turquie'
        when 'CN' then 'Chine'
        when 'IN' then 'Inde'
        when 'US' then 'États-Unis'
    end as country_name,                         -- null si pays inconnu : détecté par le test not_null

    case
        when country_code in ('FR', 'DE', 'IT', 'ES') then 'Zone euro'
        when country_code in ('PL', 'GB', 'TR')       then 'Europe hors zone euro'   -- Turquie classée en Europe pour les achats
        when country_code in ('CN', 'IN')             then 'Asie'
        when country_code in ('US')                   then 'Amérique'
    end as region                               -- null si pays inconnu : détecté par les tests

from {{ ref('stg_erp__suppliers') }}