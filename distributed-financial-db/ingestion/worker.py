# ws_ingestor.py
import asyncio, os, json, orjson, logging, signal
import websockets
from prometheus_client import Counter, Histogram, start_http_server
from kafka import KafkaProducer

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("ws-ingestor")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("MARKET_DATA_TOPIC", "market.trades.raw")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# comma-separated symbols like "btcusdt,ethusdt"
SYMBOLS = [s.strip().lower() for s in os.getenv("BINANCE_SYMBOLS", "btcusdt").split(",") if s.strip()]

# metrics
FETCH_SUCCESS = Counter("vendor_fetch_success_total", "Successful vendor fetch ops", ["vendor"])
FETCH_FAILURE = Counter("vendor_fetch_failure_total", "Failed vendor fetch ops", ["vendor"])
FETCH_LAT = Histogram("vendor_fetch_latency_seconds", "Vendor fetch latency", ["vendor"])
PUBLISH_SUCCESS = Counter("kafka_publish_success_total", "Kafka publish success", ["vendor"])
PUBLISH_FAILURE = Counter("kafka_publish_failure_total", "Kafka publish failure", ["vendor"])

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=[h.strip() for h in KAFKA_BROKER.split(",") if h.strip()],
        linger_ms=5,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda v: v.encode() if v else None,
    )

def build_ws_url(symbols):
    # Combined stream; use @trade events
    # e.g., wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    streams = "/".join(f"{s}@trade" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"

def normalize_trade(msg):
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

async def run():
    start_http_server(METRICS_PORT)
    LOG.info("Prometheus metrics on :%d/metrics", METRICS_PORT)

    producer = kafka_producer()
    url = build_ws_url(SYMBOLS)
    LOG.info("Connecting Binance WS: %s", url)
    vendor = "binance"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                while True:
                    with FETCH_LAT.labels(vendor=vendor).time():
                        raw = await ws.recv()
                    FETCH_SUCCESS.labels(vendor=vendor).inc()
                    try:
                        data = json.loads(raw)
                        payload = normalize_trade(data)
                        producer.send(TOPIC, key=payload["symbol"], value=payload)
                        PUBLISH_SUCCESS.labels(vendor=vendor).inc()
                    except Exception as pub_exc:
                        PUBLISH_FAILURE.labels(vendor=vendor).inc()
                        LOG.exception("Publish error: %s", pub_exc)
        except Exception as e:
            FETCH_FAILURE.labels(vendor=vendor).inc()
            LOG.warning("WS disconnected (%s). Reconnecting in 2s...", e)
            await asyncio.sleep(2)

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
