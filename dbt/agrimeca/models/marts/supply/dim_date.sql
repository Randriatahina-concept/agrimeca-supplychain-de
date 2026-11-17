-- dim_date : calendrier, une ligne par jour.
-- Période : de la première commande jusqu'à aujourd'hui + 1 an,
-- pour couvrir les livraisons promises dans le futur.
-- Générée par le code : ne dépend d'aucune source, sauf pour la date de début.

with bounds as (
    -- bornes du calendrier : une seule ligne, deux dates
    select
        min(order_date)                 as start_date,
        add_months(current_date(), 12)  as end_date
    from {{ ref('stg_erp__purchase_orders') }}
),

days as (
    -- une ligne par jour entre start_date et end_date
    select
        explode(sequence(start_date, end_date, interval 1 day)) as date_day
    from bounds
)

select
    date_day,
    year(date_day)                         as year,
    quarter(date_day)                      as quarter,
    month(date_day)                        as month,
    case month(date_day)
        when 1  then 'Janvier'   when 2  then 'Février'   when 3  then 'Mars'
        when 4  then 'Avril'     when 5  then 'Mai'       when 6  then 'Juin'
        when 7  then 'Juillet'   when 8  then 'Août'      when 9  then 'Septembre'
        when 10 then 'Octobre'   when 11 then 'Novembre'  when 12 then 'Décembre'
    end                                    as month_name,
    date_format(date_day, 'yyyy-MM')       as year_month,
    weekofyear(date_day)                   as week_of_year,
    weekday(date_day) + 1                  as day_of_week,   -- 1 = lundi … 7 = dimanche
    weekday(date_day) >= 5                 as is_weekend     -- samedi ou dimanche
from days