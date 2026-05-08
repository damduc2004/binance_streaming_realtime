"""
FastAPI — 7 REST endpoints
All endpoints require X-API-Key header.
All read from dbt mart views, không bao giờ query fact tables trực tiếp.

Endpoints:
  GET /health
  GET /prices/latest
  GET /prices/{symbol}/history
  GET /orderflow
  GET /orderflow/{symbol}
  GET /technical/{symbol}
  GET /alerts
"""
import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Security, Query
from fastapi.responses import FileResponse
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from prometheus_fastapi_instrumentator import Instrumentator

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://binance:binance@postgres:5432/binance_dw",
)
API_KEY = os.getenv("API_KEY", "dev-secret-key")

# asyncpg expects postgresql:// not postgresql+asyncpg://
ASYNCPG_URL = DATABASE_URL.replace("postgresql://", "")  # user:pass@host/db

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")
    return key


pool: asyncpg.Pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(
        host     = DATABASE_URL.split("@")[1].split(":")[0],
        port     = 5432,
        user     = DATABASE_URL.split("://")[1].split(":")[0],
        password = DATABASE_URL.split(":")[2].split("@")[0],
        database = DATABASE_URL.split("/")[-1],
        min_size = 2,
        max_size = 10,
        server_settings={"search_path": "public_public,public_staging,public"},
    )
    yield
    await pool.close()


app = FastAPI(
    title="Binance Streaming Analytics API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app)

_STATIC = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_STATIC), name="static")


@app.get("/dashboard", include_in_schema=False)
async def dashboard():
    return FileResponse(os.path.join(_STATIC, "dashboard.html"))


# ---------------------------------------------------------------------------
# 1. /health — không cần API key
# ---------------------------------------------------------------------------
@app.get("/health", tags=["system"])
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# 2. GET /prices/latest — giá mới nhất tất cả symbols (đọc thẳng fact table)
# ---------------------------------------------------------------------------
@app.get("/prices/latest", tags=["prices"], dependencies=[Security(verify_key)])
async def get_latest_prices():
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH latest AS (
                SELECT DISTINCT ON (s.symbol)
                    s.symbol,
                    s.display_name,
                    s.category,
                    ta.close                                    AS last_price,
                    ta.vwap,
                    ta.volume,
                    ta.trade_count,
                    ta.window_start,
                    TO_TIMESTAMP(ta.window_start / 1000)        AS updated_at
                FROM fact_trade_agg ta
                JOIN dim_symbol s      ON ta.symbol_key = s.symbol_key
                JOIN dim_window_type w ON ta.window_key  = w.window_key
                WHERE w.window_label = '1s'
                ORDER BY s.symbol, ta.window_start DESC
            ),
            prev AS (
                SELECT DISTINCT ON (s.symbol)
                    s.symbol,
                    ta.close AS prev_close
                FROM fact_trade_agg ta
                JOIN dim_symbol s      ON ta.symbol_key = s.symbol_key
                JOIN dim_window_type w ON ta.window_key  = w.window_key
                WHERE w.window_label = '1s'
                  AND ta.window_start < (
                      SELECT MAX(ta2.window_start) - 60000
                      FROM fact_trade_agg ta2
                      JOIN dim_symbol s2 ON ta2.symbol_key = s2.symbol_key
                      WHERE s2.symbol = s.symbol
                  )
                ORDER BY s.symbol, ta.window_start DESC
            )
            SELECT
                l.symbol,
                l.display_name,
                l.category,
                l.last_price,
                l.vwap,
                l.volume,
                l.trade_count,
                l.updated_at,
                COALESCE(l.last_price - p.prev_close, 0)                        AS price_change,
                CASE
                    WHEN p.prev_close IS NOT NULL AND p.prev_close != 0
                    THEN ROUND(((l.last_price - p.prev_close) / p.prev_close * 100)::numeric, 4)
                    ELSE 0
                END                                                               AS price_change_pct
            FROM latest l
            LEFT JOIN prev p ON l.symbol = p.symbol
            ORDER BY l.symbol
        """)
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 3. GET /prices/{symbol}/history — lịch sử OHLCV
# ---------------------------------------------------------------------------
@app.get("/prices/{symbol}/history", tags=["prices"], dependencies=[Security(verify_key)])
async def get_price_history(
    symbol: str,
    window: str = Query("1s", regex="^(1s|5s)$"),
    limit:  int = Query(60, ge=1, le=1440),
):
    symbol = symbol.upper()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ta.window_start,
                   ta.open, ta.high, ta.low, ta.close,
                   ta.volume, ta.vwap, ta.trade_count,
                   ta.price_change_pct
            FROM fact_trade_agg ta
            JOIN dim_symbol s     ON ta.symbol_key = s.symbol_key
            JOIN dim_window_type w ON ta.window_key = w.window_key
            WHERE s.symbol = $1 AND w.window_label = $2
            ORDER BY ta.window_start DESC
            LIMIT $3
        """, symbol, window, limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    return [dict(r) for r in reversed(rows)]  # chronological order


# ---------------------------------------------------------------------------
# 4. GET /orderflow — order flow summary tất cả symbols (30 giây gần nhất)
# ---------------------------------------------------------------------------
@app.get("/orderflow", tags=["orderflow"], dependencies=[Security(verify_key)])
async def get_orderflow_summary():
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH recent AS (
                SELECT
                    s.symbol,
                    SUM(of.buy_volume)  AS total_buy,
                    SUM(of.sell_volume) AS total_sell,
                    SUM(of.total_volume) AS total_volume,
                    SUM(of.net_flow)    AS net_flow,
                    MAX(of.window_start) AS as_of_ms
                FROM fact_order_flow of
                JOIN dim_symbol s      ON of.symbol_key = s.symbol_key
                JOIN dim_window_type w ON of.window_key  = w.window_key
                WHERE w.window_label = '1s'
                  AND of.window_start >= (EXTRACT(EPOCH FROM NOW()) * 1000 - 30000)::bigint
                GROUP BY s.symbol
            )
            SELECT
                symbol,
                total_buy,
                total_sell,
                total_volume,
                net_flow,
                CASE WHEN total_volume > 0
                     THEN ROUND((total_buy / total_volume * 100)::numeric, 2)
                     ELSE 50 END                                  AS buy_pct,
                CASE WHEN total_volume > 0
                     THEN ROUND((total_sell / total_volume * 100)::numeric, 2)
                     ELSE 50 END                                  AS sell_pct,
                CASE
                    WHEN net_flow > 0 THEN 'BUY_PRESSURE'
                    WHEN net_flow < 0 THEN 'SELL_PRESSURE'
                    ELSE 'NEUTRAL'
                END                                               AS pressure_state,
                TO_TIMESTAMP(as_of_ms / 1000)                     AS as_of
            FROM recent
            ORDER BY ABS(net_flow) DESC
        """)
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 5. GET /orderflow/{symbol} — order flow chi tiết một symbol
# ---------------------------------------------------------------------------
@app.get("/orderflow/{symbol}", tags=["orderflow"], dependencies=[Security(verify_key)])
async def get_orderflow_detail(
    symbol: str,
    limit: int = Query(60, ge=1, le=300),
):
    symbol = symbol.upper()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT of.window_start,
                   of.buy_volume, of.sell_volume, of.total_volume,
                   of.buy_count, of.sell_count,
                   of.buy_pct, of.net_flow
            FROM fact_order_flow of
            JOIN dim_symbol s     ON of.symbol_key = s.symbol_key
            JOIN dim_window_type w ON of.window_key = w.window_key
            WHERE s.symbol = $1 AND w.window_label = '1s'
            ORDER BY of.window_start DESC
            LIMIT $2
        """, symbol, limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    return [dict(r) for r in reversed(rows)]


# ---------------------------------------------------------------------------
# 6. GET /technical/{symbol} — chỉ số kỹ thuật mới nhất
# ---------------------------------------------------------------------------
@app.get("/technical/{symbol}", tags=["technical"], dependencies=[Security(verify_key)])
async def get_technical(
    symbol: str,
    limit: int = Query(1, ge=1, le=200),
):
    symbol = symbol.upper()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                s.symbol,
                TO_TIMESTAMP(ti.open_time / 1000)  AS open_time,
                TO_TIMESTAMP(kc.close_time / 1000) AS candle_time,
                kc.open, kc.high, kc.low, kc.close, kc.volume,
                kc.is_bullish,
                ti.rsi_14,
                CASE WHEN ti.rsi_14 >= 70 THEN 'OVERBOUGHT'
                     WHEN ti.rsi_14 <= 30 THEN 'OVERSOLD'
                     ELSE 'NEUTRAL' END            AS rsi_signal,
                ti.macd, ti.macd_signal, ti.macd_hist,
                CASE WHEN ti.macd > ti.macd_signal THEN 'BULLISH'
                     ELSE 'BEARISH' END            AS macd_signal_dir,
                ti.bb_upper, ti.bb_middle, ti.bb_lower, ti.bb_width,
                ti.atr_14, ti.obv
            FROM fact_technical_indicator ti
            JOIN dim_symbol s        ON ti.symbol_key   = s.symbol_key
            JOIN fact_kline_closed kc ON ti.symbol_key  = kc.symbol_key
                                     AND ti.open_time   = kc.open_time
            WHERE s.symbol = $1
            ORDER BY ti.open_time DESC
            LIMIT $2
        """, symbol, limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 7. GET /alerts — cảnh báo 24 giờ qua
# ---------------------------------------------------------------------------
@app.get("/alerts", tags=["alerts"], dependencies=[Security(verify_key)])
async def get_alerts(
    severity: Optional[str] = Query(None, regex="^(LOW|MEDIUM|HIGH)$"),
    resolved: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    filters = ["triggered_at > NOW() - INTERVAL '24 hours'"]
    params  = []

    if severity:
        params.append(severity)
        filters.append(f"severity = ${len(params)}")
    if resolved is not None:
        params.append(resolved)
        filters.append(f"is_resolved = ${len(params)}")

    params.append(limit)
    where = " AND ".join(filters)

    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                pa.alert_key,
                s.symbol,
                at.alert_code,
                at.alert_description,
                pa.severity,
                TO_TIMESTAMP(pa.triggered_at / 1000) AS triggered_at,
                pa.trigger_value,
                pa.threshold_pct,
                pa.is_resolved,
                TO_TIMESTAMP(pa.resolved_at / 1000)  AS resolved_at,
                CASE
                    WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(pa.triggered_at/1000)) BETWEEN 1 AND 8
                    THEN 'Asia'
                    WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(pa.triggered_at/1000)) BETWEEN 8 AND 16
                    THEN 'Europe'
                    ELSE 'US'
                END AS trading_session
            FROM fact_price_alert pa
            JOIN dim_symbol s     ON pa.symbol_key     = s.symbol_key
            JOIN dim_alert_type at ON pa.alert_type_key = at.alert_type_key
            WHERE pa.triggered_at > (EXTRACT(EPOCH FROM NOW()) * 1000 - 86400000)::bigint
              {('AND pa.severity = $1' if severity else '')}
              {('AND pa.is_resolved = $' + str(len(params)) if resolved is not None else '')}
            ORDER BY pa.triggered_at DESC
            LIMIT ${len(params)}
        """, *params)

    return [dict(r) for r in rows]
