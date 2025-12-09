# Distributed Financial Data Platform

**Real-Time Streaming Ingestion & Observability Stack**

A **distributed, fault-tolerant financial data platform** for ingesting, streaming, and observing **real-time market data** using **Kafka-based event pipelines**, **transactionally consistent storage**, and **production-grade observability**.

The system ingests live market data via WebSockets, publishes normalized events to Kafka topics, and exposes operational health through Prometheus and Grafana. Design decisions explicitly prioritize **correctness, consistency, and operational transparency**.

Built using **Apache Kafka**, **Dockerized ingestion workers**, **CockroachDB / PostgreSQL**, **Prometheus**, and **Grafana**.

---

## System Overview

### Core Components

**Streaming Ingestion Worker**

* Connects to live market data WebSocket feeds
* Normalizes trade and order book events
* Publishes events to Kafka topics with stable keys

**Kafka Event Backbone**

* Decouples producers and consumers
* Provides ordered, append-only logs per partition
* Enables high-throughput, low-latency streaming

**Transactional Storage**

* **PostgreSQL / CockroachDB** for durable persistence
* Serializable isolation for correctness under concurrency

**Observability Stack**

* Prometheus metrics exported by ingestion workers
* Grafana dashboards for throughput, errors, and system health

**Containerized Deployment**

* Entire stack runs via Docker Compose
* Services gated by health checks for deterministic startup

---

## Data Flow

### 1. Ingest

* WebSocket clients connect to live market feeds
* Trade ticks and top-of-book updates received in real time

### 2. Publish

* Events are normalized and published to Kafka topics:

  * `market.trades.raw`
  * `market.book_ticker.raw`

### 3. Persist (Optional Consumers)

* Downstream consumers persist data to PostgreSQL / CockroachDB
* Writes execute under **serializable transactions**

### 4. Observe

* Ingestion workers export Prometheus metrics
* Grafana visualizes ingestion rate, failures, and Kafka publish health

---

## Consistency, Sharding, and Guarantees

### Event Ordering & Serialization

* Kafka guarantees **ordered event delivery per partition**
* Events are keyed by symbol, ensuring deterministic ordering per asset
* Downstream persistence executes under **serializable isolation**
* Concurrent consumers observe results equivalent to a total transaction order

Result: **No lost updates, write skew, or partial state application**

---

### Sharding Strategy

**Kafka (Compute Sharding)**

* Keys (e.g., symbol) determine partition assignment
* Enables parallel ingestion and consumer scaling
* Maintains partition-local ordering without global coordination

**CockroachDB (Storage Sharding)**

* Data automatically split into ranges and distributed across nodes
* Ranges replicated via Raft consensus
* Rebalancing occurs transparently under load

 Result: horizontal scalability without sacrificing correctness

---

### Fault Tolerance & Replication

* **Kafka**

  * Partition replication across brokers
  * Producer retries and acknowledgement policies
* **CockroachDB**

  * Multi-replica data ranges
  * Consensus-based replication survives node failures
* **Ingestion Workers**

  * Stateless, restart-safe, horizontally scalable

 Result: component failures do not corrupt pipeline state

---

## CAP Theorem Trade-offs

This platform intentionally prioritizes **Consistency and Availability (CP)** for financial correctness.

| Layer             | CAP Behavior                          |
| ----------------- | ------------------------------------- |
| Kafka             | AP with strong per-partition ordering |
| CockroachDB       | CP (strong consistency via consensus) |
| End-to-end system | CP-biased                             |

### Practical Implication

* Under network partitions, writes may block briefly instead of returning inconsistent results
* Correctness is favored over stale or contradictory financial state

---

## Tech Stack

**Streaming**

* Apache Kafka
* ZooKeeper

**Ingestion**

* Python-based Kafka producers
* WebSocket consumers for market feeds

**Data Stores**

* PostgreSQL
* CockroachDB (replicated, sharded SQL)

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

### List Kafka topics

```bash
docker compose exec kafka bash -lc \
'kafka-topics --bootstrap-server kafka:9092 --list'
```

### Tail live trade events

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

* Prometheus: [http://localhost:9090](http://localhost:9090)
* Grafana: [http://localhost:3000](http://localhost:3000) (admin / admin)

Key metrics:

* `vendor_fetch_success_total`
* `kafka_publish_success_total`
* `vendor_fetch_failure_total`

These reveal ingestion rate, Kafka publish reliability, and shard-level failures.

---

## Scaling & Tuning

* Increase `BINANCE_SYMBOLS` for higher throughput
* Enable book ticker streams for high-frequency data
* Tune producer settings:

  * Lower latency: `KAFKA_LINGER_MS=0`
  * Higher throughput: increase batch size
* Scale WebSocket ingestion via `WS_SHARD_SIZE`

---

## Why This Project Matters

This platform demonstrates:

* **Event-driven distributed system design**
* **Strong consistency under concurrency**
* **Kafka-backed stream processing**
* **Operational observability in production systems**
* Explicit reasoning about **sharding, replication, and CAP trade-offs**

