-- =============================================================================
-- Binance Streaming Realtime — PostgreSQL Schema
-- Star Schema: 6 Fact Tables + 5 Dimension Tables
-- =============================================================================

-- ---------------------------------------------------------------------------
-- DIMENSION TABLES
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_key   SERIAL       PRIMARY KEY,
    symbol       VARCHAR(20)  NOT NULL UNIQUE,
    base_asset   VARCHAR(10)  NOT NULL,
    quote_asset  VARCHAR(10)  NOT NULL,
    display_name VARCHAR(50)  NOT NULL,
    category     VARCHAR(30)  NOT NULL,  -- Layer1 | Meme | Exchange
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_key     BIGINT      PRIMARY KEY,  -- format: YYYYMMDDHHmm
    full_timestamp   TIMESTAMP   NOT NULL,
    date             DATE        NOT NULL,
    year             SMALLINT    NOT NULL,
    quarter          SMALLINT    NOT NULL,
    month            SMALLINT    NOT NULL,
    week_of_year     SMALLINT    NOT NULL,
    day_of_month     SMALLINT    NOT NULL,
    day_of_week      SMALLINT    NOT NULL,   -- 0=Mon, 6=Sun
    is_weekend       BOOLEAN     NOT NULL,
    hour             SMALLINT    NOT NULL,
    minute           SMALLINT    NOT NULL,
    trading_session  VARCHAR(10) NOT NULL    -- Asia | Europe | US | Off
);

CREATE TABLE IF NOT EXISTS dim_window_type (
    window_key    SERIAL      PRIMARY KEY,
    window_label  VARCHAR(10) NOT NULL UNIQUE,
    duration_ms   INT         NOT NULL,
    description   TEXT
);

CREATE TABLE IF NOT EXISTS dim_kline_interval (
    interval_key    SERIAL      PRIMARY KEY,
    interval_label  VARCHAR(10) NOT NULL UNIQUE,
    duration_ms     BIGINT      NOT NULL,
    description     TEXT
);

CREATE TABLE IF NOT EXISTS dim_alert_type (
    alert_type_key  SERIAL      PRIMARY KEY,
    alert_code      VARCHAR(30) NOT NULL UNIQUE,
    description     TEXT,
    default_severity VARCHAR(10) NOT NULL  -- LOW | MEDIUM | HIGH
);

-- ---------------------------------------------------------------------------
-- FACT TABLES
-- ---------------------------------------------------------------------------

-- OHLCV aggregate từ raw trades, tumbling window 1s / 5s
CREATE TABLE IF NOT EXISTS fact_trade_agg (
    trade_agg_key    BIGSERIAL     PRIMARY KEY,
    symbol_key       INT           NOT NULL REFERENCES dim_symbol(symbol_key),
    datetime_key     BIGINT        NOT NULL REFERENCES dim_datetime(datetime_key),
    window_key       INT           NOT NULL REFERENCES dim_window_type(window_key),
    window_start     BIGINT        NOT NULL,
    window_end       BIGINT        NOT NULL,
    open             NUMERIC(18,8) NOT NULL,
    high             NUMERIC(18,8) NOT NULL,
    low              NUMERIC(18,8) NOT NULL,
    close            NUMERIC(18,8) NOT NULL,
    volume           NUMERIC(24,8) NOT NULL,
    vwap             NUMERIC(18,8) NOT NULL,
    trade_count      INT           NOT NULL,
    price_change     NUMERIC(18,8) NOT NULL,
    price_change_pct NUMERIC(10,6) NOT NULL,
    UNIQUE (symbol_key, window_start, window_key)
);

CREATE INDEX IF NOT EXISTS idx_trade_agg_symbol_time
    ON fact_trade_agg (symbol_key, window_start DESC);

-- Phân tích buy/sell pressure theo window
CREATE TABLE IF NOT EXISTS fact_order_flow (
    order_flow_key BIGSERIAL     PRIMARY KEY,
    symbol_key     INT           NOT NULL REFERENCES dim_symbol(symbol_key),
    datetime_key   BIGINT        NOT NULL REFERENCES dim_datetime(datetime_key),
    window_key     INT           NOT NULL REFERENCES dim_window_type(window_key),
    window_start   BIGINT        NOT NULL,
    buy_volume     NUMERIC(24,8) NOT NULL,
    sell_volume    NUMERIC(24,8) NOT NULL,
    total_volume   NUMERIC(24,8) NOT NULL,
    buy_count      INT           NOT NULL,
    sell_count     INT           NOT NULL,
    buy_pct        NUMERIC(6,3)  NOT NULL,
    net_flow       NUMERIC(24,8) NOT NULL,
    UNIQUE (symbol_key, window_start, window_key)
);

CREATE INDEX IF NOT EXISTS idx_order_flow_symbol_time
    ON fact_order_flow (symbol_key, window_start DESC);

-- Nến 1 phút đã đóng (is_closed = true từ Binance)
CREATE TABLE IF NOT EXISTS fact_kline_closed (
    kline_key        BIGSERIAL     PRIMARY KEY,
    symbol_key       INT           NOT NULL REFERENCES dim_symbol(symbol_key),
    datetime_key     BIGINT        NOT NULL REFERENCES dim_datetime(datetime_key),
    interval_key     INT           NOT NULL REFERENCES dim_kline_interval(interval_key),
    open_time        BIGINT        NOT NULL,
    close_time       BIGINT        NOT NULL,
    open             NUMERIC(18,8) NOT NULL,
    high             NUMERIC(18,8) NOT NULL,
    low              NUMERIC(18,8) NOT NULL,
    close            NUMERIC(18,8) NOT NULL,
    volume           NUMERIC(24,8) NOT NULL,
    trade_count      INT           NOT NULL,
    price_change     NUMERIC(18,8) NOT NULL,
    price_change_pct NUMERIC(10,6) NOT NULL,
    amplitude        NUMERIC(10,6) NOT NULL,
    is_bullish       BOOLEAN       NOT NULL,
    UNIQUE (symbol_key, open_time)
);

CREATE INDEX IF NOT EXISTS idx_kline_symbol_time
    ON fact_kline_closed (symbol_key, open_time DESC);

-- 9 chỉ số kỹ thuật tương ứng với mỗi nến đóng
CREATE TABLE IF NOT EXISTS fact_technical_indicator (
    tech_key       BIGSERIAL     PRIMARY KEY,
    symbol_key     INT           NOT NULL REFERENCES dim_symbol(symbol_key),
    datetime_key   BIGINT        NOT NULL REFERENCES dim_datetime(datetime_key),
    open_time      BIGINT        NOT NULL,
    rsi_14         NUMERIC(8,4),
    macd           NUMERIC(18,8),
    macd_signal    NUMERIC(18,8),
    macd_hist      NUMERIC(18,8),
    bb_upper       NUMERIC(18,8),
    bb_middle      NUMERIC(18,8),
    bb_lower       NUMERIC(18,8),
    bb_width       NUMERIC(10,6),
    atr_14         NUMERIC(18,8),
    obv            NUMERIC(24,8),
    UNIQUE (symbol_key, open_time)
);

CREATE INDEX IF NOT EXISTS idx_tech_symbol_time
    ON fact_technical_indicator (symbol_key, open_time DESC);

-- Trạng thái bid/ask aggregate theo giây
CREATE TABLE IF NOT EXISTS fact_spread_snapshot (
    spread_key          BIGSERIAL     PRIMARY KEY,
    symbol_key          INT           NOT NULL REFERENCES dim_symbol(symbol_key),
    datetime_key        BIGINT        NOT NULL REFERENCES dim_datetime(datetime_key),
    snapshot_time       BIGINT        NOT NULL,
    bid_price           NUMERIC(18,8) NOT NULL,
    ask_price           NUMERIC(18,8) NOT NULL,
    mid_price           NUMERIC(18,8) NOT NULL,
    spread              NUMERIC(18,8) NOT NULL,
    spread_pct          NUMERIC(10,6) NOT NULL,
    bid_qty             NUMERIC(24,8) NOT NULL,
    ask_qty             NUMERIC(24,8) NOT NULL,
    liquidity_imbalance NUMERIC(10,6) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_spread_symbol_time
    ON fact_spread_snapshot (symbol_key, snapshot_time DESC);

-- Cảnh báo bất thường thị trường
CREATE TABLE IF NOT EXISTS fact_price_alert (
    alert_key       BIGSERIAL    PRIMARY KEY,
    symbol_key      INT          NOT NULL REFERENCES dim_symbol(symbol_key),
    datetime_key    BIGINT       NOT NULL REFERENCES dim_datetime(datetime_key),
    alert_type_key  INT          NOT NULL REFERENCES dim_alert_type(alert_type_key),
    triggered_at    BIGINT       NOT NULL,
    trigger_value   NUMERIC(18,8) NOT NULL,
    threshold_pct   NUMERIC(10,6) NOT NULL,
    severity        VARCHAR(10)  NOT NULL,
    is_resolved     BOOLEAN      NOT NULL DEFAULT FALSE,
    resolved_at     BIGINT,
    UNIQUE (symbol_key, alert_type_key, triggered_at)
);

CREATE INDEX IF NOT EXISTS idx_alert_symbol_time
    ON fact_price_alert (symbol_key, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_severity
    ON fact_price_alert (severity, is_resolved);

-- ---------------------------------------------------------------------------
-- SEED DATA — Dimension tables
-- ---------------------------------------------------------------------------

INSERT INTO dim_symbol (symbol, base_asset, quote_asset, display_name, category) VALUES
    ('BTCUSDT',  'BTC',  'USDT', 'Bitcoin',   'Layer1'),
    ('ETHUSDT',  'ETH',  'USDT', 'Ethereum',  'Layer1'),
    ('BNBUSDT',  'BNB',  'USDT', 'BNB',       'Exchange'),
    ('SOLUSDT',  'SOL',  'USDT', 'Solana',    'Layer1'),
    ('XRPUSDT',  'XRP',  'USDT', 'XRP',       'Layer1'),
    ('DOGEUSDT', 'DOGE', 'USDT', 'Dogecoin',  'Meme'),
    ('ADAUSDT',  'ADA',  'USDT', 'Cardano',   'Layer1'),
    ('AVAXUSDT', 'AVAX', 'USDT', 'Avalanche', 'Layer1')
ON CONFLICT (symbol) DO NOTHING;

INSERT INTO dim_window_type (window_label, duration_ms, description) VALUES
    ('1s', 1000,  '1-second tumbling window'),
    ('5s', 5000,  '5-second tumbling window')
ON CONFLICT (window_label) DO NOTHING;

INSERT INTO dim_kline_interval (interval_label, duration_ms, description) VALUES
    ('1m', 60000, '1-minute candlestick')
ON CONFLICT (interval_label) DO NOTHING;

INSERT INTO dim_alert_type (alert_code, description, default_severity) VALUES
    ('price_spike',     'Giá thay đổi > 1% trong 5 giây',       'HIGH'),
    ('volume_surge',    'Volume > 3x trung bình 30 phút trước',  'MEDIUM'),
    ('spread_widening', 'Spread > 2x trung bình 1 giờ trước',    'MEDIUM'),
    ('low_liquidity',   'Bid+ask qty < 20% trung bình 24 giờ',   'LOW')
ON CONFLICT (alert_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- dim_datetime: generate 525,600 rows (365 ngày × 1440 phút)
-- Chạy bởi Airflow dag_init_dimensions, không seed ở đây vì quá lớn.
-- Script riêng: scripts/generate_dim_datetime.py
-- ---------------------------------------------------------------------------
