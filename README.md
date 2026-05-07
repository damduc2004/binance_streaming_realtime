# Binance Streaming Realtime Analytics Platform

> Hệ thống phân tích dữ liệu thị trường tiền điện tử theo thời gian thực — end-to-end data pipeline từ Binance WebSocket đến dashboard trực quan, với độ trễ dưới 5 giây.

---

## Tổng quan

Dự án xây dựng một **modern data stack hoàn chỉnh** để thu thập, xử lý, lưu trữ và trực quan hóa dữ liệu giao dịch của 8 đồng tiền điện tử lớn nhất từ sàn Binance:

```
Binance WebSocket
      │
      ▼
Apache Kafka  ──────────────────────────────► Azure Data Lake (Parquet)
      │
      ▼
Apache Spark Structured Streaming (4 jobs)
      │
      ▼
PostgreSQL (Star Schema, 90 ngày)
      │
      ▼
dbt Transformation  ◄──── Apache Airflow (schedule mỗi 5 phút)
      │
      ├──► Custom Dashboard (FastAPI + Chart.js)
      ├──► Apache Superset (3 dashboards, 18 charts)
      └──► Prometheus + Grafana (system monitoring)
```

**8 đồng tiền theo dõi:** BTCUSDT · ETHUSDT · BNBUSDT · SOLUSDT · XRPUSDT · DOGEUSDT · ADAUSDT · AVAXUSDT

---

## Stack công nghệ

| Tầng | Công nghệ | Phiên bản |
|---|---|---|
| Message Queue | Apache Kafka + Zookeeper + Schema Registry | 7.5.0 |
| Stream Processing | Apache Spark Structured Streaming | 3.5.1 |
| Data Warehouse | PostgreSQL | 15 |
| Transformation | dbt (dbt-postgres) | 1.7.17 |
| Orchestration | Apache Airflow | 2.x |
| API | FastAPI | 0.x |
| BI Platform | Apache Superset | latest |
| Monitoring | Prometheus + Grafana + AlertManager | latest |
| Archive | Azure Data Lake Gen2 | — |
| Container | Docker Compose | v2 |
| Language | Python | 3.11 |

---

## Cấu trúc dự án

```
binance_streaming_realtime/
│
├── ingestion/                  # Binance WebSocket producer
│   ├── producer.py             # Kết nối 3 streams: @trade, @kline_1m, @bookTicker
│   ├── config.py               # Cấu hình symbols và streams
│   └── Dockerfile
│
├── processor/                  # Spark Structured Streaming jobs
│   ├── common.py               # Shared utils: JDBC, Kafka config, dim lookups
│   ├── job1_trade_agg.py       # OHLCV + order flow aggregation (window 1s, 5s)
│   ├── job2_kline_sink.py      # Nến đóng + RSI, MACD, Bollinger Bands, ATR, OBV
│   ├── job3_spread_agg.py      # Bid/ask spread + liquidity snapshot
│   ├── job4_alert_detect.py    # Phát hiện price spike, volume surge, spread widening
│   └── job5_raw_sink.py        # Raw data → Azure Data Lake (Parquet)
│
├── api/                        # FastAPI REST API + custom dashboard
│   ├── main.py                 # 7 endpoints: /prices, /ohlcv, /orderflow, /spread, /alerts, /technical
│   ├── static/
│   │   └── dashboard.html      # Custom dashboard (Chart.js, 3 tabs)
│   └── Dockerfile
│
├── dbt/                        # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/            # stg_trade_agg, stg_kline_closed, stg_order_flow, ...
│   │   ├── intermediate/       # (reserved)
│   │   └── marts/              # mart_latest_prices, mart_technical_signals, ...
│   ├── tests/                  # Custom dbt tests
│   └── macros/
│
├── airflow/
│   └── dags/                   # 6 DAGs
│       ├── dag_init_dimensions.py    # @once — khởi tạo dim_datetime 365 ngày
│       ├── dag_dbt_transform.py      # */5 * * * * — dbt run + dbt test
│       ├── dag_health_check.py       # */5 * * * * — kiểm tra sức khỏe pipeline
│       ├── dag_data_quality.py       # 0 * * * * — kiểm tra chất lượng dữ liệu
│       ├── dag_daily_summary.py      # 5 0 * * * — tổng kết ngày
│       └── dag_retention_cleanup.py  # 0 2 * * * — xóa dữ liệu cũ > 90 ngày
│
├── postgres/
│   └── init.sql                # DDL: 6 fact + 5 dim tables, indexes
│
├── prometheus/
│   ├── prometheus.yml          # Scrape config cho 6 targets
│   ├── alert_rules.yml         # Alerting rules (Kafka lag, Spark, Postgres)
│   └── alertmanager.yml        # Email routing cho 2 nhóm alert
│
├── grafana/
│   └── dashboards/             # Grafana dashboard JSON
│
├── scripts/
│   ├── create_topics.py        # Tạo 3 Kafka topics
│   ├── generate_dim_datetime.py# Sinh 525.600 dòng dim_datetime
│   └── create_superset_dashboards.py
│
├── jars/                       # Spark JAR dependencies (git-ignored)
├── .env.example                # Template biến môi trường
├── docker-compose.yml          # 13 services
└── requirements.txt
```

---

## Yêu cầu hệ thống

| Yêu cầu | Tối thiểu | Khuyến nghị |
|---|---|---|
| RAM | 8 GB | 12 GB |
| CPU | 4 cores | 6+ cores |
| Disk | 10 GB trống | 20 GB |
| OS | Windows 10/11, macOS, Linux | Linux (Ubuntu 22.04) |
| Docker Desktop | ≥ 4.x | — |
| Docker Compose | v2 | — |

> **Lưu ý trên Linux/VM:** Cần cấu hình thêm 4 GB swap để tránh OOM khi nhiều service chạy đồng thời.

---

## Khởi động nhanh

### 1. Clone và cấu hình

```bash
git clone https://github.com/<your-username>/binance_streaming_realtime.git
cd binance_streaming_realtime

# Tạo file .env từ template
cp .env.example .env
# Điền các giá trị cần thiết (xem phần Cấu hình bên dưới)
```

### 2. Tải Spark JAR dependencies

Tạo thư mục `jars/` và tải các file JAR sau vào đó:

```
jars/
├── spark-sql-kafka-0-10_2.12-3.5.1.jar
├── kafka-clients-3.4.1.jar
├── spark-token-provider-kafka-0-10_2.12-3.5.1.jar
├── commons-pool2-2.11.1.jar
└── postgresql-42.7.3.jar
```

> Tải từ [Maven Central](https://search.maven.org/) hoặc chạy một lần với `--packages` để Spark tự tải về `~/.ivy2/`.

### 3. Khởi tạo Airflow (chỉ lần đầu)

```bash
docker compose up airflow-init
```

### 4. Khởi động toàn bộ stack

```bash
docker compose up -d
```

Chờ 2–3 phút để tất cả services sẵn sàng.

### 5. Tạo Kafka topics và khởi tạo dữ liệu chiều

```bash
# Tạo 3 Kafka topics
docker exec binance-producer python scripts/create_topics.py

# Sinh dim_datetime (525.600 dòng — chạy một lần)
# Hoặc trigger DAG dag_init_dimensions trên Airflow UI
```

### 6. Truy cập các services

| Service | URL | Thông tin đăng nhập |
|---|---|---|
| **Custom Dashboard** | http://localhost:8000/dashboard | — |
| FastAPI Docs | http://localhost:8000/docs | — |
| Airflow | http://localhost:8090 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| Apache Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Kafka UI | http://localhost:8081 | — |

---

## Cấu hình (.env)

Sao chép `.env.example` thành `.env` và điền các giá trị:

```env
# Kafka
KAFKA_BOOTSTRAP=kafka:29092

# PostgreSQL
POSTGRES_USER=binance
POSTGRES_PASSWORD=<your-password>
DATABASE_URL=postgresql://binance:<password>@postgres:5432/binance_dw

# Azure Data Lake (tùy chọn — cần cho Job 5 RawSink)
AZURE_STORAGE_ACCOUNT=<your-storage-account>
AZURE_STORAGE_KEY=<your-storage-key>
AZURE_CONTAINER=crypto-raw

# Azure Container Registry (tùy chọn — cần cho CI/CD)
ACR_LOGIN_SERVER=
ACR_USERNAME=
ACR_PASSWORD=

# Superset
SUPERSET_SECRET_KEY=<random-string>
SUPERSET_ADMIN_PASSWORD=admin

# Airflow
AIRFLOW_PASSWORD=admin

# FastAPI
API_KEY=<your-api-key>

# Grafana
GRAFANA_PASSWORD=admin

# AlertManager — email notifications
ALERT_EMAIL_FROM=your-email@gmail.com
ALERT_EMAIL_PASSWORD=<gmail-app-password>
ALERT_EMAIL_TO=your-email@gmail.com
```


---

## Mô hình dữ liệu (Star Schema)

```
                   ┌──────────────┐
                   │  dim_symbol  │  8 cặp tiền tệ
                   └──────┬───────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼──────┐  ┌───────▼──────┐  ┌──────▼───────┐
│fact_trade_agg│  │fact_kline_   │  │fact_spread_  │
│ OHLCV + flow │  │  closed      │  │  snapshot    │
│ window 1s,5s │  │ + tech indic.│  │ bid/ask/spread│
└──────────────┘  └──────────────┘  └──────────────┘

┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│fact_order_   │  │fact_technical│  │fact_price_   │
│  flow        │  │  _indicator  │  │  alert       │
│buy/sell split│  │RSI,MACD,BB.. │  │ HIGH/MED/LOW │
└──────────────┘  └──────────────┘  └──────────────┘

Dimension tables:
  dim_datetime      — 525.600 dòng (1 phút/dòng, 1 năm)
  dim_window_type   — 1s | 5s
  dim_kline_interval— 1m
  dim_alert_type    — 4 loại bất thường
```

**dbt Mart layer** (dashboard và API đọc từ đây):

| Mart | Nội dung |
|---|---|
| `mart_latest_prices` | Giá realtime mới nhất |
| `mart_technical_signals` | RSI, MACD, Bollinger Bands mới nhất |
| `mart_orderflow_summary` | Tỷ lệ và áp lực mua/bán |
| `mart_daily_performance` | Hiệu suất OHLCV theo ngày |
| `mart_spread_by_session` | Spread trung bình theo phiên Asia/Europe/US |
| `mart_alert_summary` | Cảnh báo chưa được xử lý |

---

## Các Spark jobs

| Job | Input | Output | Trigger |
|---|---|---|---|
| **Job 1** TradeAggregator | `binance.trades` | `fact_trade_agg` + `fact_order_flow` | Mỗi 5 giây |
| **Job 2** KlineSink | `binance.klines` (is_closed=true) | `fact_kline_closed` + `fact_technical_indicator` | Mỗi 1 phút |
| **Job 3** SpreadAgg | `binance.bookticker` | `fact_spread_snapshot` | Mỗi 5 giây |
| **Job 4** AlertDetect | Postgres (query 30s gần nhất) | `fact_price_alert` | Mỗi 5 giây |
| **Job 5** RawSink | 3 Kafka topics | Azure Data Lake (Parquet) | Mỗi 5 phút |

**Chỉ số kỹ thuật được tính tự động (Job 2):**
RSI-14 · MACD · MACD Signal · MACD Histogram · Bollinger Upper/Middle/Lower · ATR-14 · OBV

---

## Phát hiện bất thường

| Loại | Điều kiện | Mức độ |
|---|---|---|
| Price Spike | Giá thay đổi > 1% trong 5 giây | HIGH |
| Volume Surge | Khối lượng > 3× trung bình 30 phút trước | MEDIUM |
| Spread Widening | Spread > 2× trung bình 1 giờ trước | MEDIUM |
| Low Liquidity | Tổng bid+ask < 20% trung bình 24 giờ | LOW |

Cảnh báo mức HIGH và MEDIUM được gửi qua **email tự động** — người dùng nhận được trong vòng 5 giây kể cả khi không mở dashboard.

---

## REST API

Base URL: `http://localhost:8000`  
Authentication: Header `X-API-Key: <your-api-key>`

```
GET /health                        Trạng thái toàn bộ hệ thống
GET /prices/latest                 Giá realtime 8 đồng tiền
GET /prices/{symbol}/history       Lịch sử OHLCV (params: window, limit)
GET /technical/{symbol}            RSI, MACD, Bollinger Bands mới nhất
GET /orderflow/{symbol}            Lịch sử áp lực mua/bán
GET /spread/{symbol}               Lịch sử spread và thanh khoản
GET /alerts                        Danh sách cảnh báo (params: severity, resolved)
```

Swagger UI tại: http://localhost:8000/docs

---

## Airflow DAGs

| DAG | Schedule | Mục đích |
|---|---|---|
| `dag_init_dimensions` | @once | Khởi tạo dim_datetime và các dimension tables |
| `dag_dbt_transform` | */5 * * * * | dbt run + dbt test |
| `dag_health_check` | */5 * * * * | Kiểm tra Kafka lag, Spark, Postgres insert rate |
| `dag_data_quality` | 0 * * * * | Kiểm tra null rate, duplicate, late data |
| `dag_daily_summary` | 5 0 * * * | Tổng kết OHLCV và ranking theo ngày |
| `dag_retention_cleanup` | 0 2 * * * | Xóa dữ liệu cũ hơn 90 ngày |

---

## Monitoring

**Prometheus scrape targets:**

| Target | Metrics |
|---|---|
| Kafka (JMX) | Consumer lag, messages/sec |
| PostgreSQL Exporter | Insert rate, active connections |
| Node Exporter | CPU, RAM, disk |
| Binance Producer | WebSocket reconnect count, messages produced |
| FastAPI | HTTP request count, latency |

**AlertManager — 2 nhóm cảnh báo:**
- **market_alerts** → email người phân tích (price spike, volume surge, spread widening)
- **pipeline_alerts** → email người vận hành (Kafka lag > 10.000, Spark batch > 5s, Postgres ghi rate = 0)

---

## Fault Tolerance

| Tình huống | Cơ chế | Phục hồi |
|---|---|---|
| WebSocket Binance ngắt | Auto-reconnect sau 5 giây | < 10 giây |
| Spark job crash | Docker restart + đọc tiếp từ checkpoint | < 2 phút |
| Postgres restart | Spark kết nối lại, không mất event (Kafka còn giữ 7 ngày) | < 2 phút |
| Airflow task fail | Retry 3 lần, delay 5 phút | < 15 phút |
| Duplicate insert | ON CONFLICT DO NOTHING (idempotent) | Không có vấn đề |

---

## Kết quả đạt được

- Thu thập **liên tục 24/7** từ 8 đồng tiền, ~2–3 triệu sự kiện/ngày
- End-to-end latency **< 5 giây** từ khi giao dịch xảy ra đến khi hiển thị trên dashboard
- Tự động tính **9 chỉ số kỹ thuật** sau mỗi nến đóng
- Phát hiện và cảnh báo **4 loại bất thường** với 3 mức độ nghiêm trọng
- Lưu trữ **90 ngày** dữ liệu có cấu trúc + lưu trữ vĩnh viễn dạng file
- **Tự vận hành hoàn toàn** — 6 Airflow DAGs xử lý mọi tác vụ định kỳ
- **Tự phục hồi** khi gặp sự cố trong vòng < 2 phút
- Tái tạo toàn bộ môi trường từ đầu trong **< 5 phút** chỉ với `docker compose up -d`

---

## Giới hạn và hướng phát triển

**Hiện tại chưa có:**
- Bot giao dịch tự động (rủi ro tài chính thực)
- Mô hình dự đoán giá AI/ML
- Dữ liệu thị trường phái sinh (futures, options)
- Kubernetes orchestration

**Hướng mở rộng:**
- Thêm interval nến: 5m, 15m, 1h (chỉ cần thêm Kafka stream)
- Mở rộng lên 50+ đồng tiền (chỉ cần cập nhật config)
- Tích hợp cảnh báo qua Telegram bot
- Triển khai mô hình anomaly detection bằng ML

