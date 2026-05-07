-- Giá mới nhất của tất cả 8 đồng tiền (dùng window 1 giây)
-- Dashboard 1: Price Ticker panel
WITH ranked AS (
    SELECT
        ta.symbol,
        ta.display_name,
        ta.category,
        ta.close,
        ta.vwap,
        ta.volume,
        ta.price_change,
        ta.price_change_pct,
        ta.trade_count,
        ta.window_start,
        ta.trading_session,
        ROW_NUMBER() OVER (
            PARTITION BY ta.symbol
            ORDER BY ta.window_start DESC
        ) AS rn
    FROM {{ ref('stg_trade_agg') }} ta
    WHERE ta.window_type = '1s'
)
SELECT
    r.symbol,
    r.display_name,
    r.category,
    r.close                                                    AS last_price,
    r.vwap,
    r.volume,
    r.price_change,
    r.price_change_pct,
    r.trade_count,
    TO_TIMESTAMP(r.window_start / 1000)                        AS updated_at,
    r.trading_session
FROM ranked r
WHERE r.rn = 1
