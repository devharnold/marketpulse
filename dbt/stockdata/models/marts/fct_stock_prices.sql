-- Final Data Mart

SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    price_change,
    daily_return
FROM {{ ref('int_stock_returns') }}