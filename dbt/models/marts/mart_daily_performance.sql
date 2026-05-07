{{ config(materialized='table', post_hook='ANALYZE {{ this }}') }}

-- Hiệu suất hàng ngày mỗi symbol — 30 ngày qua
-- Dashboard 3: Performance Heatmap, Top Movers, Bullish/Bearish Ratio
SELECT
    symbol,
    date,
    ROUND(MIN(low)::NUMERIC, 8)                          AS day_low,
    ROUND(MAX(high)::NUMERIC, 8)                         AS day_high,
    ROUND(
        (MAX(CASE WHEN rn_asc = 1 THEN open END))::NUMERIC, 8
    )                                                     AS day_open,
    ROUND(
        (MAX(CASE WHEN rn_desc = 1 THEN close END))::NUMERIC, 8
    )                                                     AS day_close,
    ROUND(SUM(volume)::NUMERIC, 8)                        AS day_volume,
    SUM(trade_count)                                      AS day_trade_count,
    COUNT(*)                                              AS candle_count,
    SUM(CASE WHEN is_bullish THEN 1 ELSE 0 END)           AS bullish_candles,
    SUM(CASE WHEN NOT is_bullish THEN 1 ELSE 0 END)       AS bearish_candles,
    ROUND(AVG(amplitude)::NUMERIC, 4)                     AS avg_amplitude
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY open_time ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY open_time DESC) AS rn_desc
    FROM {{ ref('stg_kline_closed') }}
    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
) sub
GROUP BY symbol, date
ORDER BY date DESC, symbol
