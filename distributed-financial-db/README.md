Steps to run : 

1. Clone repo

2. Install docker daeomon and leave it running

3. cd to project directory and docker compose up -d

# Runbook: Bring Up the Distributed Financial Stack & Validate Kafka Topics

This guide walks you through bringing up the full stack (ZooKeeper, Kafka, Kafka Exporter, CockroachDB, Postgres, Prometheus, Grafana, Ingestion Worker) and verifying that market data is flowing into Kafka topics.

---

## 1) Prerequisites

* Docker + Docker Compose
* `git`, `curl`, and optionally `jq`
* Open outbound internet access (WebSocket + HTTPS) to Binance US if you’re in the U.S.

Directory layout (example):

```
project-root/
  docker-compose.yml
  ingestion/
    Dockerfile
    worker.py
    .env
  monitoring/
    prometheus.yml
    grafana/
      provisioning/  # (optional dashboards/datasources)
```

---

## 2) Configure environment for the ingestion worker (./ingestion/.env)

Recommended starter config:

```env
# WebSocket base (use .us if you’re in the U.S.)
BINANCE_WS_BASE=wss://stream.binance.us:9443

# A safe, active symbol set on Binance US
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,ADAUSDT,DOGEUSDT,LTCUSDT,AVAXUSDT,LINKUSDT,MATICUSDT

# Optional: high-frequency book ticker stream
ENABLE_BOOK_TICKER=true
MARKET_TRADES_TOPIC=market.trades.raw
MARKET_BOOK_TOPIC=market.book_ticker.raw

# WebSocket behavior
WS_SHARD_SIZE=100
WS_PING_INTERVAL=20
WS_PING_TIMEOUT=20

# Kafka producer tuning
KAFKA_BROKER=kafka:9092
KAFKA_LINGER_MS=10
KAFKA_BATCH_SIZE=65536
KAFKA_COMPRESSION=lz4
KAFKA_ACKS=1
KAFKA_MAX_IN_FLIGHT=5
KAFKA_RETRIES=10

# Prometheus metrics port
METRICS_PORT=8000

# Validation (set VALIDATE_ONLY=true to run checks then exit)
VALIDATE_ON_START=true
VALIDATE_ONLY=false

# For validation ping (optional but useful)
BINANCE_HOSTS=api.binance.us
```

> **Tip:** Do **not** quote the `BINANCE_SYMBOLS` line. Avoid trailing spaces.

---

## 3) Docker Compose (project-root/docker-compose.yml)

* Ensure the ingestion worker service has:

  * `env_file: - ./ingestion/.env`
  * `depends_on: kafka: { condition: service_healthy }`
* Expose Prometheus (9090) and Grafana (3000) if you’ll use them locally.

> If you already applied the earlier patches, you’re set. Otherwise, add `env_file` and the Kafka health dependency to your compose file.

---

## 4) Build and start the stack

From project root:

```bash
docker compose up -d --build
```

Wait for health checks:

```bash
docker compose ps
docker compose logs -f kafka | egrep "started \(kafka.server.KafkaServer\)|[Hh]ealth"
```

---

## 5) Verify the ingestion worker picked up your env

```bash
docker compose exec -T ingestion-worker sh -lc 'printf "WS_BASE=%s\nSYMBOLS=%s\nBOOK=%s\n" "$BINANCE_WS_BASE" "$BINANCE_SYMBOLS" "$ENABLE_BOOK_TICKER"'
```

Expected (example):

```
WS_BASE=wss://stream.binance.us:9443
SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,ADAUSDT,DOGEUSDT,LTCUSDT,AVAXUSDT,LINKUSDT,MATICUSDT
BOOK=true
```

---

## 6) Confirm services are healthy

```bash
# Kafka
docker compose logs --tail=100 kafka

# Ingestion worker
docker compose logs --tail=200 ingestion-worker
# Look for lines like:
#   "Trade shard 1 connecting ... wss://stream.binance.us:9443/stream?streams=..."
#   "Validation: Topic 'market.trades.raw' available with partitions: [0]"

# Prometheus & metrics
curl -s http://localhost:8000/metrics | egrep 'vendor_fetch_success_total|kafka_publish_success_total|vendor_fetch_failure_total'
```

If you see `HTTP 451` in logs, you’re likely hitting the global `.com` endpoint from the U.S. Ensure `BINANCE_WS_BASE=wss://stream.binance.us:9443`.

---

## 7) Create/inspect Kafka topics (optional)

List topics:

```bash
docker compose exec kafka bash -lc 'kafka-topics --bootstrap-server kafka:9092 --list'
```

Create topics explicitly (if auto-create is disabled):

```bash
docker compose exec kafka bash -lc '
  kafka-topics --bootstrap-server kafka:9092 --create --topic market.trades.raw --partitions 1 --replication-factor 1 || true
  kafka-topics --bootstrap-server kafka:9092 --create --topic market.book_ticker.raw --partitions 1 --replication-factor 1 || true
'
```

Describe a topic:

```bash
docker compose exec kafka bash -lc 'kafka-topics --bootstrap-server kafka:9092 --describe --topic market.trades.raw'
```

---

## 8) Live-tail **new** messages (Kafka consumers)

### Trades (per-match ticks)

```bash
docker compose exec kafka bash -lc \
  'kafka-console-consumer --bootstrap-server kafka:9092 \
   --topic market.trades.raw --property print.key=true'
```

Expected output (examples):

```
BTCUSDT	{"vendor":"binance","symbol":"BTCUSDT","event_time":...,"trade_id":...,"price":...,"qty":...,"is_buyer_maker":false}
ETHUSDT	{"vendor":"binance","symbol":"ETHUSDT",...}
```

### Book ticker (top-of-book; higher rate)

```bash
docker compose exec kafka bash -lc \
  'kafka-console-consumer --bootstrap-server kafka:9092 \
   --topic market.book_ticker.raw --property print.key=true'
```

Expected output (examples):

```
BTCUSDT	{"vendor":"binance","symbol":"BTCUSDT","event_time":...,"update_id":...,"bid_price":...,"bid_qty":...,"ask_price":...,"ask_qty":...}
```

### With timestamps in the consumer

```bash
docker compose exec kafka bash -lc \
  "kafka-console-consumer --bootstrap-server kafka:9092 --topic market.trades.raw \
   --formatter kafka.tools.DefaultMessageFormatter \
   --property print.timestamp=true --property print.key=true --property print.value=true"
```

### Quick per-symbol counters (for activity)

```bash
docker compose exec kafka bash -lc \
  'kafka-console-consumer --bootstrap-server kafka:9092 --topic market.trades.raw --property print.key=true \
   | awk -F"\t" '\''{c[$1]++} NR%50==0{for(k in c)print strftime("%H:%M:%S"), k, c[k]; print "---"; fflush()}'\'''
```

> **Note:** Omit `--from-beginning` to see only new events. Add it if you want to replay from the start of the log.

---

## 9) Prometheus & Grafana (optional)

* Prometheus: [http://localhost:9090](http://localhost:9090)
* Grafana: [http://localhost:3000](http://localhost:3000) (admin/admin by default in the example compose)

  * Add Prometheus as a data source (URL: `http://prometheus:9090` from inside Docker, or `http://localhost:9090` from host if port mapped).
  * Build quick panels for:

    * `vendor_fetch_success_total` (rate by shard)
    * `kafka_publish_success_total` (rate by shard)
    * `vendor_fetch_failure_total`

---

## 10) Troubleshooting Cheatsheet

**A. WebSocket 451 (legal block)**

* Symptom: `server rejected WebSocket connection: HTTP 451`
* Fix: Use `BINANCE_WS_BASE=wss://stream.binance.us:9443` and U.S.-listed symbols.

**B. No messages in Kafka consumer**

* Check metrics: `vendor_fetch_success_total` (frames arriving?) and `kafka_publish_success_total` (publishing?)
* Tail worker logs for errors: `docker compose logs -f ingestion-worker`
* Run a direct WS smoke test inside the worker:

```bash
docker compose exec -T ingestion-worker python - <<'PY'
import asyncio, os, websockets
base=os.getenv("BINANCE_WS_BASE","wss://stream.binance.us:9443")
url=f"{base}/stream?streams=btcusdt@trade"
async def main():
    async with websockets.connect(url) as ws:
        print("connected to", url)
        for _ in range(3):
            msg=await ws.recv()
            print("sample:", msg[:160], "...")
asyncio.run(main())
PY
```

**C. Producer error: acks must be int**

* Symptom: `ValueError: convert value: '1' required argument is not an integer`
* Fix: Ensure `KAFKA_ACKS=1` (no quotes) in `.env`. Worker handles `-1`/`all` as well.

**D. DNS/connection flapping to `kafka:9092`**

* Ensure `depends_on: kafka: { condition: service_healthy }` for the worker.
* If needed, create a named network for stable DNS and attach all services.

**E. Module not found (e.g., orjson)**

* Ensure the worker image installs required deps in `requirements.txt` and rebuild: `docker compose up -d --build ingestion-worker`.

---

## 11) Scaling message volume

* Add more U.S.-listed symbols to `BINANCE_SYMBOLS`.
* Keep `ENABLE_BOOK_TICKER=true` for high-frequency top-of-book updates.
* Tune producer for lower latency: `KAFKA_LINGER_MS=0`, `KAFKA_COMPRESSION=none`.
* Use smaller `WS_SHARD_SIZE` (e.g., 25) to open more sockets if needed.

---

## 12) One-command Validation Mode (optional)

If you want a pass/fail before running, set in `.env`:

```env
VALIDATE_ONLY=true
VALIDATE_ON_START=true
```

Then:

```bash
docker compose up -d --force-recreate ingestion-worker
# Expect exit code 0 on success; inspect logs for the full summary
```

Switch back to `VALIDATE_ONLY=false` for normal streaming.

---

### You’re done!

With the consumer tails running, you’ll see live trade ticks on `market.trades.raw` and frequent top-of-book updates on `market.book_ticker.raw`. Use the Prometheus counters to verify throughput and health over time.
