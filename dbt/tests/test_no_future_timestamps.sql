-- Không có record với timestamp trong tương lai (> 10 giây tolerance)
SELECT symbol, window_start
FROM {{ ref('stg_trade_agg') }}
WHERE window_start > (EXTRACT(EPOCH FROM NOW()) * 1000 + 10000)
