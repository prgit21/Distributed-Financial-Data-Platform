"""Ingestion worker for fetching vendor market data (Binance) and publishing to Kafka."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)


# -----------------------------
# Env helpers
# -----------------------------
def _parse_env_list(env_var: str, default: str = "") -> List[str]:
    value = os.getenv(env_var, default)
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_kafka_api_version() -> Optional[Tuple[int, ...]]:
    raw = os.getenv("KAFKA_API_VERSION", "2.5.0")  # from your broker logs
    try:
        parts = tuple(int(p) for p in raw.split("."))
        return parts if parts else None
    except Exception:
        return None


# -----------------------------
# Configuration
# -----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Try these hosts in order until one returns valid JSON (no cross-host redirect)
BINANCE_HOSTS: List[str] = _parse_env_list(
    "BINANCE_HOSTS",
    "data-api.binance.vision,api.binance.com,api1.binance.com,api-gcp.binance.com,api.binance.us",
)
BINANCE_PATH = os.getenv("BINANCE_PATH", "/api/v3/ticker/24hr")

BINANCE_SYMBOLS = _parse_env_list("BINANCE_SYMBOLS") or _parse_env_list("YFINANCE_SYMBOLS")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
MARKET_DATA_TOPIC = os.getenv("MARKET_DATA_TOPIC") or os.getenv("YFINANCE_TOPIC", "market-data-binance")

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "2"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "5"))

KAFKA_API_VERSION = _parse_kafka_api_version()
KAFKA_CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "30"))
KAFKA_CONNECT_INITIAL_DELAY = float(os.getenv("KAFKA_CONNECT_INITIAL_DELAY", "1.0"))
KAFKA_CONNECT_MAX_DELAY = float(os.getenv("KAFKA_CONNECT_MAX_DELAY", "5.0"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "15"))
HTTP_TOTAL_RETRIES = int(os.getenv("HTTP_TOTAL_RETRIES", "2"))  # per-host before rotation


# -----------------------------
# Prometheus metrics
# -----------------------------
FETCH_SUCCESS = Counter("vendor_fetch_success_total", "Number of successful vendor fetch operations", ["vendor"])
FETCH_FAILURE = Counter("vendor_fetch_failure_total", "Number of failed vendor fetch operations", ["vendor"])
FETCH_LATENCY = Histogram("vendor_fetch_latency_seconds", "Latency of vendor fetch operations", ["vendor"])
PUBLISH_SUCCESS = Counter("kafka_publish_success_total", "Number of successful Kafka publish operations", ["vendor"])
PUBLISH_FAILURE = Counter("kafka_publish_failure_total", "Number of failed Kafka publish operations", ["vendor"])
BINANCE_HTTP_ERRORS = Counter("binance_http_errors_total", "HTTP errors from Binance (status codes)", ["status_code"])
BINANCE_HOST_ATTEMPTS = Counter("binance_host_attempts_total", "Attempts made against a given Binance host", ["host"])


# -----------------------------
# Utils
# -----------------------------
def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            ts = value / 1000.0 if value > 1e12 else float(value)  # ms -> s
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except (OverflowError, ValueError):
            return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    return None


def _normalize_payload(vendor: str, symbol: str, timestamp: Optional[str], pricing: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "vendor": vendor,
        "symbol": symbol,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "open": _to_float(pricing.get("open")),
        "high": _to_float(pricing.get("high")),
        "low": _to_float(pricing.get("low")),
        "close": _to_float(pricing.get("close")) or _to_float(pricing.get("price")),
        "price": _to_float(pricing.get("price")) or _to_float(pricing.get("close")),
        "volume": _to_float(pricing.get("volume")),
        "currency": pricing.get("currency"),
        "raw": pricing,
    }


# -----------------------------
# Kafka
# -----------------------------
def _create_producer() -> KafkaProducer:
    LOGGER.info("Connecting KafkaProducer to %s", KAFKA_BROKER)
    delay = KAFKA_CONNECT_INITIAL_DELAY
    last_exc: Optional[Exception] = None
    for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            kwargs = dict(
                bootstrap_servers=[KAFKA_BROKER],
                request_timeout_ms=20000,
                reconnect_backoff_ms=500,
                reconnect_backoff_max_ms=5000,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8") if v else None,
            )
            if KAFKA_API_VERSION:
                kwargs["api_version"] = KAFKA_API_VERSION
            return KafkaProducer(**kwargs)
        except Exception as e:
            last_exc = e
            LOGGER.warning("Kafka not ready (attempt %d/%d): %s", attempt, KAFKA_CONNECT_RETRIES, e)
            time.sleep(delay)
            delay = min(delay * 1.5, KAFKA_CONNECT_MAX_DELAY)
    raise RuntimeError(f"Failed to create KafkaProducer after retries: {last_exc}")


def _publish_message(producer: KafkaProducer, topic: str, payload: Dict[str, Any]) -> None:
    vendor = payload.get("vendor", "unknown")
    try:
        producer.send(topic, key=payload.get("symbol"), value=payload)
        PUBLISH_SUCCESS.labels(vendor=vendor).inc()
    except Exception as exc:
        PUBLISH_FAILURE.labels(vendor=vendor).inc()
        LOGGER.exception("Failed to publish message to %s: %s", topic, exc)
        raise


# -----------------------------
# HTTP session (Binance) with retry
# -----------------------------
def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": os.getenv("HTTP_USER_AGENT", "market-ingestor/1.0")})
    retry = Retry(
        total=HTTP_TOTAL_RETRIES,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = _build_session()


def _read_retry_after(resp: requests.Response) -> float:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return 0.0
    try:
        return float(ra)
    except Exception:
        return 0.0


def _same_host(a: str, b: str) -> bool:
    return urlparse(a).netloc.lower() == urlparse(b).netloc.lower()


def _get_json_one_host(host: str, path: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
    """Fetch JSON from one host. Allow redirects (compat with old requests), then block cross-host hops."""
    base = f"https://{host}{path if path.startswith('/') else '/' + path}"
    BINANCE_HOST_ATTEMPTS.labels(host=host).inc()

    # DO NOT pass allow_redirects (breaks older requests/adapter combos). Use default behavior.
    r = SESSION.get(base, params=params, timeout=HTTP_TIMEOUT)

    # Block cross-host redirects (e.g., to www.binance.com/en HTML)
    final_url = r.url
    if r.history:
        # If any redirect in the chain changes host, reject
        chain = [h.headers.get("Location", "") or h.url for h in r.history] + [final_url]
        hosts_in_chain = [urlparse(u).netloc for u in chain if u]
        if any(not _same_host(base, u) for u in chain):
            LOGGER.error("Cross-host redirect blocked: %s -> %s (chain hosts=%s)", base, final_url, hosts_in_chain)
            BINANCE_HTTP_ERRORS.labels(status_code=str(getattr(r, "status_code", 0) or 0)).inc()
            raise RuntimeError("Cross-host redirect blocked")

    ct = r.headers.get("Content-Type", "")

    if r.status_code == 429:
        BINANCE_HTTP_ERRORS.labels(status_code=str(r.status_code)).inc()
        backoff = max(RETRY_BACKOFF_SECONDS, _read_retry_after(r))
        LOGGER.warning("Binance 429 at %s; backing off %.1fs", r.url, backoff)
        time.sleep(backoff)
        raise RuntimeError("Rate limited")

    if not r.ok:
        BINANCE_HTTP_ERRORS.labels(status_code=str(r.status_code)).inc()
        LOGGER.error("Binance %s -> %s %s; body[:200]=%r", r.url, r.status_code, ct, r.text[:200])
        raise RuntimeError(f"HTTP {r.status_code}")

    if "application/json" not in ct.lower():
        LOGGER.error("Unexpected Content-Type for %s: %s; body[:200]=%r", r.url, ct, r.text[:200])
        raise RuntimeError("Non-JSON response")

    LOGGER.debug("Fetched %d bytes from %s", len(r.content), r.url)
    try:
        return r.json()
    except Exception:
        LOGGER.error("JSON parse error for %s; body[:200]=%r", r.url, r.text[:200])
        raise


def _get_json_any_host(
    hosts: Sequence[str],
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    per_host_attempts: int = 2,
) -> Any:
    last_exc: Optional[Exception] = None
    for host in hosts:
        for _ in range(per_host_attempts):
            try:
                return _get_json_one_host(host, path, params=params)
            except Exception as e:
                last_exc = e
                time.sleep(0.4)
        LOGGER.info("Host %s did not yield JSON; rotating to next host", host)
    raise RuntimeError(f"All hosts failed for {path}: {last_exc}")


# -----------------------------
# Binance fetchers
# -----------------------------
def fetch_binance_symbol(symbol: str) -> Dict[str, Any]:
    return _get_json_any_host(BINANCE_HOSTS, BINANCE_PATH, params={"symbol": symbol.upper()})


def fetch_binance_tickers() -> List[Dict[str, Any]]:
    if BINANCE_SYMBOLS:
        return [fetch_binance_symbol(s) for s in BINANCE_SYMBOLS]
    data = _get_json_any_host(BINANCE_HOSTS, BINANCE_PATH)
    if isinstance(data, dict):
        for key in ("data", "symbols", "tickers", "items"):
            nested = data.get(key)
            if isinstance(nested, list):
                data = nested
                break
        else:
            raise ValueError("Unexpected response format from Binance endpoint (dict)")
    if not isinstance(data, list):
        raise ValueError(f"Binance endpoint response was not a list, got {type(data)}")
    return data


# -----------------------------
# Normalization
# -----------------------------
def _normalize_binance_ticker(ticker: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(ticker.get("symbol", "")).strip()
    if not symbol:
        raise ValueError("Ticker payload missing symbol")

    timestamp = _coerce_timestamp(
        ticker.get("closeTime") or ticker.get("eventTime") or ticker.get("close_time") or ticker.get("event_time")
    )

    pricing = {
        "open": ticker.get("openPrice") or ticker.get("open_price"),
        "high": ticker.get("highPrice") or ticker.get("high_price"),
        "low": ticker.get("lowPrice") or ticker.get("low_price"),
        "close": ticker.get("prevClosePrice") or ticker.get("prev_close_price"),
        "price": ticker.get("lastPrice") or ticker.get("last_price"),
        "volume": ticker.get("volume"),
        "currency": ticker.get("quoteAsset") or ticker.get("quote_asset"),
    }

    payload = _normalize_payload("binance", symbol, timestamp, pricing)
    payload["raw"] = ticker
    return payload


# -----------------------------
# Retry wrapper
# -----------------------------
def fetch_with_retry(vendor: str, func, *args, **kwargs):
    delay = RETRY_BACKOFF_SECONDS
    last_exception: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            FETCH_SUCCESS.labels(vendor=vendor).inc()
            FETCH_LATENCY.labels(vendor=vendor).observe(time.time() - start_time)
            return result
        except Exception as exc:
            last_exception = exc
            FETCH_FAILURE.labels(vendor=vendor).inc()
            FETCH_LATENCY.labels(vendor=vendor).observe(time.time() - start_time)
            LOGGER.exception(
                "Failed to fetch %s data on attempt %s/%s: %s", vendor, attempt, MAX_RETRIES, exc
            )
            if attempt >= MAX_RETRIES:
                break
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    assert last_exception is not None
    raise last_exception


# -----------------------------
# Iteration & main loop
# -----------------------------
def _iter_binance_payloads() -> Iterable[Dict[str, Any]]:
    try:
        tickers = fetch_with_retry("binance", fetch_binance_tickers)
    except Exception as exc:
        LOGGER.error("Failed to fetch Binance tickers: %s", exc)
        return

    symbol_filter = {s.upper() for s in BINANCE_SYMBOLS} if BINANCE_SYMBOLS else None

    for ticker in tickers:
        if not isinstance(ticker, dict):
            LOGGER.error("Unexpected ticker shape: %r", ticker)
            continue
        symbol = str(ticker.get("symbol", "")).upper()
        if symbol_filter and symbol not in symbol_filter:
            continue
        try:
            yield _normalize_binance_ticker(ticker)
        except Exception as exc:
            LOGGER.error("Skipping Binance symbol %s after normalization error: %s", symbol or "<unknown>", exc)


def run_worker(stop_event: threading.Event) -> None:
    start_http_server(METRICS_PORT)
    LOGGER.info("Prometheus metrics server listening on port %s", METRICS_PORT)
    producer = _create_producer()
    cycle_count = 0
    try:
        while not stop_event.is_set():
            for payload in _iter_binance_payloads():
                try:
                    _publish_message(producer, MARKET_DATA_TOPIC, payload)
                except Exception:
                    LOGGER.error("Error publishing payload for vendor %s", payload.get("vendor"))
            cycle_count += 1
            if cycle_count % FLUSH_EVERY == 0:
                LOGGER.debug("Flushing Kafka producer")
                producer.flush()
            stop_event.wait(POLL_INTERVAL)
    finally:
        LOGGER.info("Shutting down ingestion worker")
        try:
            producer.flush()
        finally:
            producer.close()


def main() -> None:
    stop_event = threading.Event()

    def _signal_handler(signum, frame):  # noqa: ARG001
        LOGGER.info("Received signal %s; initiating shutdown", signum)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _signal_handler)

    run_worker(stop_event)


if __name__ == "__main__":
    main()
