-- Un taux par devise et par jour calendaire : week-ends et fériés complétés par le dernier taux publié
with rates as (
    select currency, rate_date, rate
    from {{ ref('stg_ecb__exchange_rates') }}
),

bounds as (
    select min(rate_date) as first_day, current_date() as last_day
    from rates
),

calendar as (
    select explode(sequence(first_day, last_day)) as rate_date    -- tous les jours, sans trou
    from bounds
),

grid as (
    select c.currency, cal.rate_date                              -- chaque devise × chaque jour
    from (select distinct currency from rates) as c
    cross join calendar as cal
),

filled as (
    select
        g.currency,
        g.rate_date,
        r.rate is not null as is_published,                       -- vrai : taux BCE du jour ; faux : reporté
        last(r.rate, true) over (
            partition by g.currency
            order by g.rate_date
            rows between unbounded preceding and current row
        ) as rate                                                 -- dernier taux connu à cette date
    from grid as g
    left join rates as r
        on r.currency = g.currency and r.rate_date = g.rate_date
)

select currency || '-' || cast(rate_date as string) as fx_key, currency, rate_date, is_published, rate
from filled

union all

select 'EUR-' || cast(rate_date as string), 'EUR', rate_date, true, cast(1 as decimal(18, 6))
from calendar                                                     -- l'euro vaut toujours 1 euro