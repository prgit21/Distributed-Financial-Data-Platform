# worker.py
import asyncio
import os
import json
import orjson
import logging
import signal
import socket
import ssl
import time
import urllib.request
from typing import List, Iterable

import websockets
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge, start_http_server


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOG = logging.getLogger("ws-ingestor")


# -----------------------------
# Environment / Config
# -----------------------------
def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _list_from_env(name: str, default: str = "") -> List[str]:
    return [x.strip() for x in os.getenv(name, default).split(",") if x.strip()]


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443")
TOPIC = os.getenv("MARKET_DATA_TOPIC", "market.trades.raw")
METRICS_PORT = _int("METRICS_PORT", 8000)

# Symbols (case-insensitive, normalized to lowercase)
SYMBOLS = [s.lower() for s in _list_from_env("BINANCE_SYMBOLS", "btcusdt")]
# Remove duplicates while preserving order
_seen = set()
SYMBOLS = [s for s in SYMBOLS if not (s in _seen or _seen.add(s))]

# WS sharding + keepalives
WS_SHARD_SIZE = _int("WS_SHARD_SIZE", 100)           # symbols per connection
WS_PING_INTERVAL = _float("WS_PING_INTERVAL", 20.0)
WS_PING_TIMEOUT = _float("WS_PING_TIMEOUT", 20.0)

# Kafka producer tuning
KAFKA_LINGER_MS = _int("KAFKA_LINGER_MS", 10)
KAFKA_BATCH_SIZE = _int("KAFKA_BATCH_SIZE", 65536)    # 64 KB
KAFKA_COMPRESSION = os.getenv("KAFKA_COMPRESSION", "lz4")  # lz4|snappy|gzip|None
KAFKA_ACKS = os.getenv("KAFKA_ACKS", "1")
KAFKA_MAX_IN_FLIGHT = _int("KAFKA_MAX_IN_FLIGHT", 5)
KAFKA_RETRIES = _int("KAFKA_RETRIES", 10)

# Validation toggles
VALIDATE_ON_START = os.getenv("VALIDATE_ON_START", "true").lower() in ("1", "true", "yes")
VALIDATE_ONLY = os.getenv("VALIDATE_ONLY", "false").lower() in ("1", "true", "yes")

# Optional list of fallback Binance hosts to test basic reachability
BINANCE_HOSTS = _list_from_env(
    "BINANCE_HOSTS",
    "",
)


# -----------------------------
# Metrics
# -----------------------------
FETCH_SUCCESS = Counter("vendor_fetch_success_total", "Successful vendor fetch ops", ["vendor", "shard"])
FETCH_FAILURE = Counter("vendor_fetch_failure_total", "Failed vendor fetch ops", ["vendor", "shard"])
FETCH_LAT = Histogram("vendor_fetch_latency_seconds", "Vendor fetch latency", ["vendor", "shard"])
PUBLISH_SUCCESS = Counter("kafka_publish_success_total", "Kafka publish success", ["vendor", "shard"])
PUBLISH_FAILURE = Counter("kafka_publish_failure_total", "Kafka publish failure", ["vendor", "shard"])

# Validation gauges (1 = OK, 0 = FAIL)
VAL_KAFKA_CONNECT = Gauge("validation_kafka_connect_ok", "Kafka connectivity OK (1/0)")
VAL_KAFKA_TOPIC = Gauge("validation_kafka_topic_ok", "Kafka topic available OK (1/0)", ["topic"])
VAL_SYMBOLS_OK = Gauge("validation_symbols_ok", "Symbols parsed and within limits (1/0)")
VAL_BINANCE_PING = Gauge("validation_binance_ping_ok", "Binance API ping OK (1/0)", ["host"])


# -----------------------------
# Helpers
# -----------------------------
def chunk_iter(items: List[str], n: int) -> Iterable[List[str]]:
    if n <= 0:
        n = 100
    for i in range(0, len(items), n):
        yield items[i : i + n]
def _acks_from_env(name: str, default: str = "1") -> int:
   
   v = os.getenv(name, default).strip().lower()
   if v in ("all", "-1"):
        return -1
   try:
        return int(v)  # 0 or 1
   except Exception:
        LOG.warning("Invalid %s=%r; defaulting to 1", name, v)
        return 1

KAFKA_ACKS = _acks_from_env("KAFKA_ACKS", "1")

def build_ws_url(symbols: List[str]) -> str:
    # Combined stream; use @trade events
    # e.g., wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    streams = "/".join(f"{s}@trade" for s in symbols)
    return f"{BINANCE_WS_BASE}/stream?streams={streams}"


def normalize_trade(msg: dict) -> dict:
    # Binance combined stream wraps payload under "data"
    d = msg.get("data", msg)
    return {
        "vendor": "binance",
        "symbol": d.get("s"),
        "event_time": d.get("E"),
        "trade_id": d.get("t"),
        "price": float(d.get("p")),
        "qty": float(d.get("q")),
        "is_buyer_maker": bool(d.get("m")),
    }


def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[h.strip() for h in KAFKA_BROKER.split(",") if h.strip()],
        linger_ms=KAFKA_LINGER_MS,
        batch_size=KAFKA_BATCH_SIZE,
        compression_type=(None if KAFKA_COMPRESSION.lower() == "none" else KAFKA_COMPRESSION),
        acks=KAFKA_ACKS,
        max_in_flight_requests_per_connection=KAFKA_MAX_IN_FLIGHT,
        retries=KAFKA_RETRIES,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda v: v.encode() if v else None,
    )


# -----------------------------
# Validation Routines
# -----------------------------
def validate_symbols() -> bool:
    """
    Ensures: non-empty, deduped, shard size sane, per-connection streams <= 1024.
    """
    ok = True
    if not SYMBOLS:
        LOG.error("Validation: No symbols found (BINANCE_SYMBOLS empty?)")
        ok = False

    # Binance limit: 1024 streams per WS connection (we use 1 stream per symbol).
    if WS_SHARD_SIZE > 1024:
        LOG.error("Validation: WS_SHARD_SIZE=%d > 1024 limit", WS_SHARD_SIZE)
        ok = False

    # Keep it reasonable
    if WS_SHARD_SIZE <= 0:
        LOG.error("Validation: WS_SHARD_SIZE must be > 0")
        ok = False

    VAL_SYMBOLS_OK.set(1 if ok else 0)
    LOG.info("Validation: %d symbols, shard_size=%d, shards=%d",
             len(SYMBOLS), WS_SHARD_SIZE, max(1, (len(SYMBOLS) + WS_SHARD_SIZE - 1) // WS_SHARD_SIZE))
    return ok


def validate_kafka_connectivity() -> bool:
    """
    Try an admin connection and query partitions for the target topic.
    """
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=[h.strip() for h in KAFKA_BROKER.split(",") if h.strip()],
            client_id="ws-ingestor-validate",
        )
        # If the cluster is reachable, this call succeeds
        topics = admin.list_topics()
        LOG.info("Validation: Kafka reachable, %d topics visible", len(topics))
        VAL_KAFKA_CONNECT.set(1)
        return True
    except Exception as e:
        LOG.exception("Validation: Kafka connectivity failed: %s", e)
        VAL_KAFKA_CONNECT.set(0)
        return False


def validate_kafka_topic() -> bool:
    """
    Check that the topic is queryable from a Producer's perspective.
    """
    try:
        producer = kafka_producer()
        parts = producer.partitions_for(TOPIC)
        if parts is None:
            LOG.warning("Validation: Topic '%s' not found (partitions_for returned None)", TOPIC)
            VAL_KAFKA_TOPIC.labels(topic=TOPIC).set(0)
            return False
        LOG.info("Validation: Topic '%s' available with partitions: %s", TOPIC, sorted(parts))
        VAL_KAFKA_TOPIC.labels(topic=TOPIC).set(1)
        return True
    except KafkaError as ke:
        LOG.exception("Validation: partitions_for() failed: %s", ke)
        VAL_KAFKA_TOPIC.labels(topic=TOPIC).set(0)
        return False
    except Exception as e:
        LOG.exception("Validation: Error checking topic: %s", e)
        VAL_KAFKA_TOPIC.labels(topic=TOPIC).set(0)
        return False


def _dns_lookup(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except Exception:
        return False


def _https_ping(url: str, timeout: float = 5.0) -> bool:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def validate_binance_connectivity() -> bool:
    """
    Basic checks: DNS resolution on candidate hosts + GET /api/v3/ping on at least one host.
    """
    ok_any = False
    for host in BINANCE_HOSTS:
        dns_ok = _dns_lookup(host)
        if not dns_ok:
            LOG.warning("Validation: DNS lookup failed for %s", host)
            VAL_BINANCE_PING.labels(host=host).set(0)
            continue

        # Choose https endpoint if it's the main API; otherwise just mark DNS as OK
        if "api.binance" in host:
            url = f"https://{host}/api/v3/ping"
            http_ok = _https_ping(url, timeout=5.0)
            VAL_BINANCE_PING.labels(host=host).set(1 if http_ok else 0)
            LOG.info("Validation: Binance ping %s -> %s", host, "OK" if http_ok else "FAIL")
            ok_any = ok_any or http_ok
        else:
            # For non-REST hosts (like data-api.binance.vision), a DNS pass is a good indicator
            VAL_BINANCE_PING.labels(host=host).set(1)
            LOG.info("Validation: Binance DNS %s -> OK", host)
            ok_any = True

    if not ok_any:
        LOG.error("Validation: All Binance host checks failed")
    return ok_any


def run_all_validations() -> bool:
    sym_ok = validate_symbols()
    kafka_ok = validate_kafka_connectivity()
    topic_ok = validate_kafka_topic() if kafka_ok else False
    bin_ok = validate_binance_connectivity()
    overall = sym_ok and kafka_ok and topic_ok and bin_ok
    LOG.info("Validation summary: symbols=%s kafka=%s topic=%s binance=%s => overall=%s",
             sym_ok, kafka_ok, topic_ok, bin_ok, overall)
    return overall


# -----------------------------
# WebSocket Consumers
# -----------------------------
async def _consume_symbols(symbols: List[str], producer: KafkaProducer, shard_id: int):
    vendor = "binance"
    url = build_ws_url(symbols)
    LOG.info("Shard %s connecting (%d symbols): %s", shard_id, len(symbols), url)

    while True:
        try:
            async with websockets.connect(
                url, ping_interval=WS_PING_INTERVAL, ping_timeout=WS_PING_TIMEOUT
            ) as ws:
                while True:
                    with FETCH_LAT.labels(vendor=vendor, shard=str(shard_id)).time():
                        raw = await ws.recv()
                    FETCH_SUCCESS.labels(vendor=vendor, shard=str(shard_id)).inc()

                    try:
                        # orjson is fast; keep json for fallback/debug if needed
                        data = orjson.loads(raw)
                        payload = normalize_trade(data)
                        producer.send(TOPIC, key=payload["symbol"], value=payload)
                        PUBLISH_SUCCESS.labels(vendor=vendor, shard=str(shard_id)).inc()
                    except Exception as pub_exc:
                        PUBLISH_FAILURE.labels(vendor=vendor, shard=str(shard_id)).inc()
                        LOG.exception("Publish error (shard %s): %s", shard_id, pub_exc)

        except Exception as e:
            FETCH_FAILURE.labels(vendor=vendor, shard=str(shard_id)).inc()
            LOG.warning("WS shard %s disconnected (%s). Reconnecting in 2s...", shard_id, e)
            await asyncio.sleep(2)


# -----------------------------
# Main run loop
# -----------------------------
async def run():
    start_http_server(METRICS_PORT)
    LOG.info("Prometheus metrics on :%d/metrics", METRICS_PORT)

    if VALIDATE_ON_START or VALIDATE_ONLY:
        ok = run_all_validations()
        if VALIDATE_ONLY:
            # Exit after validations (useful for CI or manual diagnostics)
            LOG.info("VALIDATE_ONLY=true; exiting with status %d", 0 if ok else 1)
            raise SystemExit(0 if ok else 1)
        if not ok:
            LOG.warning("Validation had failures; continuing to run (set VALIDATE_ONLY=true to gate startup)")

    producer = kafka_producer()
    tasks = []
    for shard_id, syms in enumerate(chunk_iter(SYMBOLS, WS_SHARD_SIZE), 1):
        tasks.append(asyncio.create_task(_consume_symbols(syms, producer, shard_id)))
    await asyncio.gather(*tasks)


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop = asyncio.Event()

    def _stop(*_):
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _stop)

    loop.create_task(run())
    loop.run_until_complete(stop.wait())


if __name__ == "__main__":
    main()
