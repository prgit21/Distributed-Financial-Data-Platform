# Distributed Financial Data Platform

**Real-Time Streaming Ingestion & Observability Stack**

A **distributed, fault-tolerant financial data platform** for ingesting, streaming, and observing **real-time market data** using **Kafka-based event pipelines**, **transactionally consistent storage**, and **production-style observability**.

The system ingests live market data via WebSockets, publishes normalized events to Kafka topics, and exposes operational health via Prometheus and Grafana.

Built with **Apache Kafka**, **Dockerized ingestion workers**, **CockroachDB / PostgreSQL**, **Prometheus**, and **Grafana**.

---

## System Overview

### Core Components

* **Streaming ingestion worker**

  * Connects to live market data WebSocket feeds
  * Normalizes and publishes events to Kafka topics
* **Kafka event backbone**

  * Decouples producers and consumers
  * Supports high-throughput, low-latency message streams
* **Transactional storage**

  * PostgreSQL / CockroachDB for durable, strongly consistent storage
* **Observability stack**

  * Prometheus metrics exported by ingestion workers
  * Grafana dashboards for throughput, lag, and failure analysis
* **Containerized deployment**

  * Entire stack runs via Docker Compose with health checks

---

## Data Flow

1. **Ingest**

   * WebSocket clients connect to market data feeds
   * Trades and book updates received in real time

2. **Publish**

   * Events are normalized and published to **Kafka topics**

     * `market.trades.raw`
     * `market.book_ticker.raw`

3. **Persist (optional consumers)**

   * Downstream consumers can persist to PostgreSQL / CockroachDB
   * Serializable transactions supported for correctness

4. **Observe**

   * Ingestion workers export Prometheus metrics
   * Grafana dashboards visualize throughput, failures, and system health

---

## Tech Stack

**Streaming**

* Apache Kafka
* ZooKeeper

**Ingestion**

* Python-based Kafka producers
* WebSocket consumers (market data)

**Data Stores**

* PostgreSQL
* CockroachDB (sharded, replicated SQL)

**Observability**

* Prometheus
* Grafana

**Infrastructure**

* Docker
* Docker Compose

---

## Running the System

### Prerequisites

* Docker + Docker Compose
* Git, curl
* Outbound WebSocket + HTTPS access to Binance US (for U.S. users)

---

### Quick Start

```bash
git clone <repo>
cd <project-root>

docker compose up -d --build
```

Verify services:

```bash
docker compose ps
docker compose logs -f kafka
```

---

## Environment Configuration (Ingestion Worker)

Edit `ingestion/.env`:

```env
BINANCE_WS_BASE=wss://stream.binance.us:9443
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT,DOGEUSDT

MARKET_TRADES_TOPIC=market.trades.raw
MARKET_BOOK_TOPIC=market.book_ticker.raw

KAFKA_BROKER=kafka:9092
KAFKA_LINGER_MS=10
KAFKA_COMPRESSION=lz4
KAFKA_ACKS=1

METRICS_PORT=8000
VALIDATE_ON_START=true
VALIDATE_ONLY=false
```

---

## Validate the Pipeline

### Confirm worker configuration

```bash
docker compose exec ingestion-worker sh -lc \
'echo $BINANCE_WS_BASE && echo $BINANCE_SYMBOLS'
```

### Check Kafka topics

```bash
docker compose exec kafka bash -lc \
'kafka-topics --bootstrap-server kafka:9092 --list'
```

### Tail live events

```bash
docker compose exec kafka bash -lc \
'kafka-console-consumer --bootstrap-server kafka:9092 \
 --topic market.trades.raw --property print.key=true'
```

Sample output:

```text
BTCUSDT {"symbol":"BTCUSDT","price":...,"qty":...}
```

---

## Observability

* **Prometheus**: [http://localhost:9090](http://localhost:9090)
* **Grafana**: [http://localhost:3000](http://localhost:3000) (admin / admin)

Key metrics:

* `vendor_fetch_success_total`
* `kafka_publish_success_total`
* `vendor_fetch_failure_total`

Use these to validate:

* Ingestion rate
* Kafka publish reliability
* Failure patterns across shards

---

## Scaling & Tuning

* Increase `BINANCE_SYMBOLS` for higher volume
* Enable book ticker for high-frequency updates
* Tune producer settings:

  * Lower latency: `KAFKA_LINGER_MS=0`
  * Higher throughput: increase batch size
* Scale WebSocket shards via `WS_SHARD_SIZE`

---
