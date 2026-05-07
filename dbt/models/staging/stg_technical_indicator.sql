SELECT
    ti.tech_key,
    s.symbol,
    ti.open_time,
    ti.rsi_14::FLOAT         AS rsi_14,
    ti.macd::FLOAT           AS macd,
    ti.macd_signal::FLOAT    AS macd_signal,
    ti.macd_hist::FLOAT      AS macd_hist,
    ti.bb_upper::FLOAT       AS bb_upper,
    ti.bb_middle::FLOAT      AS bb_middle,
    ti.bb_lower::FLOAT       AS bb_lower,
    ti.bb_width::FLOAT       AS bb_width,
    ti.atr_14::FLOAT         AS atr_14,
    ti.obv::FLOAT            AS obv,
    -- Derived signals
    CASE
        WHEN ti.rsi_14 > 70 THEN 'overbought'
        WHEN ti.rsi_14 < 30 THEN 'oversold'
        ELSE 'neutral'
    END AS rsi_signal,
    CASE
        WHEN ti.macd > ti.macd_signal THEN 'bullish'
        WHEN ti.macd < ti.macd_signal THEN 'bearish'
        ELSE 'neutral'
    END AS macd_signal_dir
FROM {{ source('public', 'fact_technical_indicator') }} ti
JOIN {{ source('public', 'dim_symbol') }} s ON ti.symbol_key = s.symbol_key
WHERE s.is_active = true
