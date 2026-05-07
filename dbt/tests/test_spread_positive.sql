-- Spread phải luôn dương
SELECT symbol, snapshot_time, spread
FROM {{ ref('stg_spread_snapshot') }}
WHERE spread <= 0
