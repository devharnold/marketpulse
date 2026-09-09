with stockdata as (
    select * 
    from {{ source('raw', 'stock_prices') }}
),
-- cast the data types to their preference
renamed as (
    select
    symbol,
    date,
    CAST(open AS NUMERIC) AS open,
    CAST(high AS NUMERIC) AS high,
    CAST(low AS NUMERIC) AS low,
    CAST(close AS NUMERIC) AS close,
    CAST(volume AS BIGINT) AS volume

    from stockdata
)

select * from renamed

--{% if target.name == 'postgres' %}
-- where date >= '2026-01-01' and date <= '2026-02-01'
-- {% endif %}