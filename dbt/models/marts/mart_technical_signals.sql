-- Chỉ số kỹ thuật mới nhất mỗi symbol
-- Dashboard 2: MACD, RSI, Bollinger Bands panels
WITH ranked AS (
    SELECT
        ti.*,
        k.open, k.high, k.low, k.close, k.volume, k.is_bullish, k.amplitude,
        ROW_NUMBER() OVER (PARTITION BY ti.symbol ORDER BY ti.open_time DESC) AS rn
    FROM {{ ref('stg_technical_indicator') }} ti
    JOIN {{ ref('stg_kline_closed') }} k
        ON ti.symbol = k.symbol AND ti.open_time = k.open_time
)
SELECT
    symbol,
    open_time,
    TO_TIMESTAMP(open_time / 1000)  AS candle_time,
    open, high, low, close, volume, is_bullish, amplitude,
    rsi_14,
    rsi_signal,
    macd,
    macd_signal,
    macd_hist,
    macd_signal_dir,
    bb_upper,
    bb_middle,
    bb_lower,
    bb_width,
    atr_14,
    obv,
    -- Composite signal
    CASE
        WHEN rsi_signal = 'oversold'   AND macd_signal_dir = 'bullish' THEN 'strong_buy'
        WHEN rsi_signal = 'overbought' AND macd_signal_dir = 'bearish' THEN 'strong_sell'
        WHEN macd_signal_dir = 'bullish' THEN 'buy'
        WHEN macd_signal_dir = 'bearish' THEN 'sell'
        ELSE 'neutral'
    END AS composite_signal
FROM ranked
WHERE rn = 1
