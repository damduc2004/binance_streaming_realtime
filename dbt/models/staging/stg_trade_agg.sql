-- Staging: clean + type-cast fact_trade_agg
SELECT
    ta.trade_agg_key,
    s.symbol,
    s.display_name,
    s.category,
    wt.window_label AS window_type,
    ta.window_start,
    ta.window_end,
    ta.open::FLOAT         AS open,
    ta.high::FLOAT         AS high,
    ta.low::FLOAT          AS low,
    ta.close::FLOAT        AS close,
    ta.volume::FLOAT       AS volume,
    ta.vwap::FLOAT         AS vwap,
    ta.trade_count,
    ta.price_change::FLOAT AS price_change,
    ta.price_change_pct::FLOAT AS price_change_pct,
    d.trading_session,
    d.hour,
    d.day_of_week,
    d.is_weekend
FROM {{ source('public', 'fact_trade_agg') }} ta
JOIN {{ source('public', 'dim_symbol') }}      s   ON ta.symbol_key  = s.symbol_key
JOIN {{ source('public', 'dim_window_type') }} wt  ON ta.window_key  = wt.window_key
JOIN {{ source('public', 'dim_datetime') }}    d   ON ta.datetime_key = d.datetime_key
WHERE s.is_active = true
