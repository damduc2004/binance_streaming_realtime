-- Order flow summary mỗi symbol — 5 phút gần nhất
-- Dashboard 1: Buy/Sell Pressure gauge
WITH recent AS (
    SELECT
        symbol,
        SUM(buy_volume)   AS total_buy,
        SUM(sell_volume)  AS total_sell,
        SUM(net_flow)     AS net_flow,
        AVG(buy_pct)      AS avg_buy_pct,
        MAX(window_start) AS latest_window
    FROM {{ ref('stg_order_flow') }}
    WHERE window_type = '1s'
      AND window_start >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes') * 1000
    GROUP BY symbol
)
SELECT
    symbol,
    total_buy,
    total_sell,
    total_buy + total_sell                          AS total_volume,
    net_flow,
    ROUND(avg_buy_pct::NUMERIC, 2)                  AS buy_pct,
    ROUND((100 - avg_buy_pct)::NUMERIC, 2)          AS sell_pct,
    CASE
        WHEN avg_buy_pct > 55 THEN 'buy_dominated'
        WHEN avg_buy_pct < 45 THEN 'sell_dominated'
        ELSE 'balanced'
    END                                             AS pressure_state,
    TO_TIMESTAMP(latest_window / 1000)              AS as_of
FROM recent
