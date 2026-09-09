-- Calculate prices changes and returns

WITH stock_data AS (
    SELECT
        symbol,
        date,
        close
    FROM {{ ref('stg_stockdata') }}
),

stock_returns AS (
    SELECT
        symbol,
        date,
        close,

        close - LAG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS price_change,
        (
            close / LAG(close) OVER (
                PARTITION BY symbol
                ORDER BY date
            ) - 1
        ) * 100 AS daily_return

    FROM stock_data
)

SELECT *
FROM stock_returns