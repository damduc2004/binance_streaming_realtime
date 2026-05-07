SELECT
    k.kline_key,
    s.symbol,
    k.open_time,
    k.close_time,
    k.open::FLOAT            AS open,
    k.high::FLOAT            AS high,
    k.low::FLOAT             AS low,
    k.close::FLOAT           AS close,
    k.volume::FLOAT          AS volume,
    k.trade_count,
    k.price_change::FLOAT    AS price_change,
    k.price_change_pct::FLOAT AS price_change_pct,
    k.amplitude::FLOAT       AS amplitude,
    k.is_bullish,
    d.trading_session,
    d.date,
    d.hour
FROM {{ source('public', 'fact_kline_closed') }} k
JOIN {{ source('public', 'dim_symbol') }}   s ON k.symbol_key  = s.symbol_key
JOIN {{ source('public', 'dim_datetime') }} d ON k.datetime_key = d.datetime_key
WHERE s.is_active = true
