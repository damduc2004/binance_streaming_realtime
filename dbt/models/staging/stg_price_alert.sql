SELECT
    pa.alert_key,
    s.symbol,
    at.alert_code,
    at.description       AS alert_description,
    pa.triggered_at,
    pa.trigger_value::FLOAT AS trigger_value,
    pa.threshold_pct::FLOAT AS threshold_pct,
    pa.severity,
    pa.is_resolved,
    pa.resolved_at,
    d.trading_session,
    d.date
FROM {{ source('public', 'fact_price_alert') }} pa
JOIN {{ source('public', 'dim_symbol') }}     s  ON pa.symbol_key     = s.symbol_key
JOIN {{ source('public', 'dim_alert_type') }} at ON pa.alert_type_key = at.alert_type_key
JOIN {{ source('public', 'dim_datetime') }}   d  ON pa.datetime_key   = d.datetime_key
WHERE s.is_active = true
