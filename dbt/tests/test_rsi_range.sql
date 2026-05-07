-- RSI phải nằm trong [0, 100] khi không null
SELECT symbol, open_time, rsi_14
FROM {{ ref('stg_technical_indicator') }}
WHERE rsi_14 IS NOT NULL
  AND (rsi_14 < 0 OR rsi_14 > 100)
