{{ config(materialized='table', post_hook='ANALYZE {{ this }}') }}

-- Spread trung bình theo phiên giao dịch — 24 giờ qua
-- Dashboard 3: Volume by Session, Spread Heatmap
SELECT
    symbol,
    trading_session,
    ROUND(AVG(spread_pct)::NUMERIC, 4)           AS avg_spread_pct,
    ROUND(AVG(spread)::NUMERIC, 8)               AS avg_spread_abs,
    ROUND(AVG(liquidity_imbalance)::NUMERIC, 4)  AS avg_imbalance,
    ROUND(AVG(bid_qty + ask_qty)::NUMERIC, 4)    AS avg_total_qty,
    COUNT(*)                                      AS snapshot_count
FROM {{ ref('stg_spread_snapshot') }}
WHERE snapshot_time > EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') * 1000
GROUP BY symbol, trading_session
ORDER BY avg_spread_pct DESC
