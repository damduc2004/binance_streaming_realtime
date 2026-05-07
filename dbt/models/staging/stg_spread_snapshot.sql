SELECT
    ss.spread_key,
    s.symbol,
    ss.snapshot_time,
    ss.bid_price::FLOAT          AS bid_price,
    ss.ask_price::FLOAT          AS ask_price,
    ss.mid_price::FLOAT          AS mid_price,
    ss.spread::FLOAT             AS spread,
    ss.spread_pct::FLOAT         AS spread_pct,
    ss.bid_qty::FLOAT            AS bid_qty,
    ss.ask_qty::FLOAT            AS ask_qty,
    ss.liquidity_imbalance::FLOAT AS liquidity_imbalance,
    d.trading_session,
    d.hour,
    d.date
FROM {{ source('public', 'fact_spread_snapshot') }} ss
JOIN {{ source('public', 'dim_symbol') }}   s ON ss.symbol_key  = s.symbol_key
JOIN {{ source('public', 'dim_datetime') }} d ON ss.datetime_key = d.datetime_key
WHERE ss.spread > 0
  AND s.is_active = true
