-- Cảnh báo đang active + 24 giờ qua
-- Dashboard 1: Active Alerts table
SELECT
    alert_key,
    symbol,
    alert_code,
    alert_description,
    severity,
    TO_TIMESTAMP(triggered_at / 1000)   AS triggered_at,
    trigger_value,
    threshold_pct,
    is_resolved,
    CASE
        WHEN resolved_at IS NOT NULL
        THEN TO_TIMESTAMP(resolved_at / 1000)
    END                                 AS resolved_at,
    trading_session,
    EXTRACT(EPOCH FROM NOW()) * 1000 - triggered_at AS age_ms
FROM {{ ref('stg_price_alert') }}
WHERE triggered_at > EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours') * 1000
ORDER BY triggered_at DESC
