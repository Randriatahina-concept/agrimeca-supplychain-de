-- Un taux par devise et par jour publié BCE (unités de devise pour 1 EUR), dernière lecture retenue
with ranked as (
    select
        upper(currency)               as currency,
        to_date(rate_date)            as rate_date,
        cast(rate as decimal(18, 6))  as rate,
        _run_id,
        _ingest_ts,
        row_number() over (
            partition by upper(currency), to_date(rate_date)   -- les runs du Job se chevauchent de 10 jours
            order by _ingest_ts desc                           -- la lecture la plus récente gagne
        ) as version_rank
    from {{ source('bronze', 'ecb_exchange_rates') }}
    where try_cast(rate as double) > 0                         -- écarte un taux vide ou non numérique
)

select
    currency || '-' || cast(rate_date as string) as fx_key,    -- clé unique : devise + jour
    * except (version_rank)
from ranked
where version_rank = 1