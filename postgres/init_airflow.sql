-- Tạo database riêng cho Airflow metadata (cùng PostgreSQL instance)
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO binance;
