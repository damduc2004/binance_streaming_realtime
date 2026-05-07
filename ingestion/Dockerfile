FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir confluent-kafka websocket-client python-dotenv prometheus-client

COPY ingestion/ ingestion/

# Prometheus metrics server chạy ở port 8001
ENV PYTHONUNBUFFERED=1
EXPOSE 8001

CMD ["python", "-m", "ingestion.producer"]
