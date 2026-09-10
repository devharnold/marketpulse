-- Determine the stock indicators of the stock data

WITH stock_data AS (
    SELECT
        symbol,
        date,
        close
    FROM {{ ref('stg_stockdata') }}
),

stock_indicators AS (
    SELECT
        symbol,
        date,
        close,

        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20

    FROM stock_data
)

SELECT *
FROM stock_indicators