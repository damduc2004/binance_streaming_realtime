SELECT
    of.order_flow_key,
    s.symbol,
    wt.window_label AS window_type,
    of.window_start,
    of.buy_volume::FLOAT    AS buy_volume,
    of.sell_volume::FLOAT   AS sell_volume,
    of.total_volume::FLOAT  AS total_volume,
    of.buy_count,
    of.sell_count,
    of.buy_pct::FLOAT       AS buy_pct,
    (100 - of.buy_pct)::FLOAT AS sell_pct,
    of.net_flow::FLOAT      AS net_flow,
    d.trading_session,
    d.hour
FROM {{ source('public', 'fact_order_flow') }} of
JOIN {{ source('public', 'dim_symbol') }}      s   ON of.symbol_key  = s.symbol_key
JOIN {{ source('public', 'dim_window_type') }} wt  ON of.window_key  = wt.window_key
JOIN {{ source('public', 'dim_datetime') }}    d   ON of.datetime_key = d.datetime_key
WHERE s.is_active = true
