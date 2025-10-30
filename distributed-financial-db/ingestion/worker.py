"""Ingestion worker for fetching vendor market data and publishing to Kafka."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import yfinance as yf
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)


def _parse_env_list(env_var: str, default: str = "") -> List[str]:
    value = os.getenv(env_var, default)
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
YFINANCE_SYMBOLS = _parse_env_list("YFINANCE_SYMBOLS")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

YFINANCE_TOPIC = os.getenv("YFINANCE_TOPIC", "market-data-yfinance")

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "2"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "5"))


FETCH_SUCCESS = Counter(
    "vendor_fetch_success_total",
    "Number of successful vendor fetch operations",
    labelnames=("vendor",),
)
FETCH_FAILURE = Counter(
    "vendor_fetch_failure_total",
    "Number of failed vendor fetch operations",
    labelnames=("vendor",),
)
FETCH_LATENCY = Histogram(
    "vendor_fetch_latency_seconds",
    "Latency of vendor fetch operations",
    labelnames=("vendor",),
)
PUBLISH_SUCCESS = Counter(
    "kafka_publish_success_total",
    "Number of successful Kafka publish operations",
    labelnames=("vendor",),
)
PUBLISH_FAILURE = Counter(
    "kafka_publish_failure_total",
    "Number of failed Kafka publish operations",
    labelnames=("vendor",),
)


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _create_producer() -> KafkaProducer:
    LOGGER.info("Connecting KafkaProducer to %s", KAFKA_BROKER)
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
    )


def _normalize_payload(
    vendor: str,
    symbol: str,
    timestamp: Optional[str],
    pricing: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "vendor": vendor,
        "symbol": symbol,
        "timestamp": timestamp or datetime.utcnow().isoformat(),
        "open": _to_float(pricing.get("open")),
        "high": _to_float(pricing.get("high")),
        "low": _to_float(pricing.get("low")),
        "close": _to_float(pricing.get("close"))
        or _to_float(pricing.get("price")),
        "price": _to_float(pricing.get("price"))
        or _to_float(pricing.get("close")),
        "volume": _to_float(pricing.get("volume")),
        "currency": pricing.get("currency"),
        "raw": pricing,
    }


def _coerce_timestamp(value: Any) -> Optional[str]:
    if value is None:
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


def _safe_get_fast_info(symbol: str, ticker: yf.Ticker) -> Dict[str, Any]:
    try:
        fast_info = ticker.get_fast_info()
    except AttributeError:
        try:
            fast_info = ticker.fast_info
        except Exception:  # pylint: disable=broad-except
            LOGGER.debug("fast_info lookup failed for %s", symbol, exc_info=True)
            fast_info = None
    except Exception:  # pylint: disable=broad-except
        LOGGER.debug("get_fast_info failed for %s", symbol, exc_info=True)
        fast_info = None

    if not fast_info:
        return {}

    if hasattr(fast_info, "as_dict"):
        info_dict = fast_info.as_dict()
    elif isinstance(fast_info, dict):
        info_dict = fast_info
    else:
        info_dict = getattr(fast_info, "_data", {}) or {}

    serializable: Dict[str, Any] = {}
    for key, value in info_dict.items():
        try:
            serializable[key] = value.item() if hasattr(value, "item") else value
        except Exception:  # pylint: disable=broad-except
            continue
    return serializable


def _pricing_from_fast_info(info_dict: Dict[str, Any]) -> Dict[str, Any]:
    if not info_dict:
        return {}
    return {
        "price": info_dict.get("last_price")
        or info_dict.get("regular_market_price"),
        "open": info_dict.get("regular_market_open")
        or info_dict.get("previous_close"),
        "high": info_dict.get("day_high")
        or info_dict.get("regular_market_day_high"),
        "low": info_dict.get("day_low")
        or info_dict.get("regular_market_day_low"),
        "close": info_dict.get("previous_close")
        or info_dict.get("last_price"),
        "volume": info_dict.get("last_volume")
        or info_dict.get("regular_market_volume"),
        "currency": info_dict.get("currency")
        or info_dict.get("regular_market_currency"),
    }


def _history_snapshot(symbol: str, ticker: yf.Ticker) -> Optional[Dict[str, Any]]:
    try:
        history = ticker.history(period="5d", interval="1d")
    except Exception:  # pylint: disable=broad-except
        LOGGER.debug("history lookup failed for %s", symbol, exc_info=True)
        return None

    if history.empty:
        return None

    latest = history.dropna(how="all")
    if latest.empty:
        return None
    latest_row = latest.iloc[-1]

    pricing = {
        "open": latest_row.get("Open"),
        "high": latest_row.get("High"),
        "low": latest_row.get("Low"),
        "close": latest_row.get("Close"),
        "price": latest_row.get("Close"),
        "volume": latest_row.get("Volume"),
    }

    timestamp = _coerce_timestamp(latest_row.name)

    raw: Dict[str, Any] = {"timestamp": timestamp}
    for field in ("Open", "High", "Low", "Close", "Volume"):
        try:
            raw[field] = _to_float(latest_row.get(field))
        except Exception:  # pylint: disable=broad-except
            continue

    return {
        "timestamp": timestamp,
        "pricing": pricing,
        "raw": raw,
    }


def fetch_yfinance_quote(symbol: str) -> Dict[str, Any]:
    ticker = yf.Ticker(symbol)
    info_dict = _safe_get_fast_info(symbol, ticker)
    pricing = _pricing_from_fast_info(info_dict)
    timestamp: Optional[str] = None

    history = None
    if not any(
        pricing.get(field) is not None for field in ("price", "close", "open", "high", "low")
    ):
        history = _history_snapshot(symbol, ticker)
        if not history:
            raise RuntimeError(f"No pricing data returned for {symbol}")
        pricing.update({k: v for k, v in history["pricing"].items() if v is not None})
        timestamp = history.get("timestamp")
    else:
        history = _history_snapshot(symbol, ticker)
        if history:
            timestamp = history.get("timestamp")
            for key, value in history["pricing"].items():
                if pricing.get(key) is None and value is not None:
                    pricing[key] = value

    payload = _normalize_payload(
        "yfinance", symbol, timestamp or datetime.utcnow().isoformat(), pricing
    )

    raw: Dict[str, Any] = {"fast_info": info_dict}
    if history:
        raw["history"] = history["raw"]
    payload["raw"] = raw
    return payload


def fetch_with_retry(vendor: str, func, *args, **kwargs) -> Dict[str, Any]:
    delay = RETRY_BACKOFF_SECONDS
    last_exception: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            FETCH_SUCCESS.labels(vendor=vendor).inc()
            FETCH_LATENCY.labels(vendor=vendor).observe(time.time() - start_time)
            return result
        except Exception as exc:  # pylint: disable=broad-except
            last_exception = exc
            FETCH_FAILURE.labels(vendor=vendor).inc()
            FETCH_LATENCY.labels(vendor=vendor).observe(time.time() - start_time)
            LOGGER.exception(
                "Failed to fetch %s data on attempt %s/%s: %s",
                vendor,
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt >= MAX_RETRIES:
                break
            time.sleep(delay)
            delay *= 2
    assert last_exception is not None
    raise last_exception


def _publish_message(producer: KafkaProducer, topic: str, payload: Dict[str, Any]) -> None:
    vendor = payload.get("vendor", "unknown")
    try:
        producer.send(topic, key=payload.get("symbol"), value=payload)
        PUBLISH_SUCCESS.labels(vendor=vendor).inc()
    except Exception as exc:  # pylint: disable=broad-except
        PUBLISH_FAILURE.labels(vendor=vendor).inc()
        LOGGER.exception("Failed to publish message to %s: %s", topic, exc)
        raise


def _iter_yfinance_payloads() -> Iterable[Dict[str, Any]]:
    for symbol in YFINANCE_SYMBOLS:
        if not symbol:
            continue
        yield fetch_with_retry("yfinance", fetch_yfinance_quote, symbol)


def run_worker(stop_event: threading.Event) -> None:
    start_http_server(METRICS_PORT)
    LOGGER.info("Prometheus metrics server listening on port %s", METRICS_PORT)
    producer = _create_producer()
    cycle_count = 0
    try:
        while not stop_event.is_set():
            for payload in _iter_yfinance_payloads():
                try:
                    _publish_message(producer, YFINANCE_TOPIC, payload)
                except Exception:
                    LOGGER.error(
                        "Error publishing payload for vendor %s", payload.get("vendor")
                    )
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

    def _signal_handler(signum, frame):  # pylint: disable=unused-argument
        LOGGER.info("Received signal %s; initiating shutdown", signum)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _signal_handler)

    run_worker(stop_event)


if __name__ == "__main__":
    main()
