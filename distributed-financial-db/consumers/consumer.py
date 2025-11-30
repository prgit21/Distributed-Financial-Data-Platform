import json
import logging
import os
import signal
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Optional

import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("consumer")


# ----------------------
# Metrics
# ----------------------
MESSAGES_CONSUMED = Counter(
    "consumer_messages_consumed_total",
    "Total messages consumed by topic",
    labelnames=["topic"],
)

BATCH_DURATION = Histogram(
    "consumer_batch_duration_seconds",
    "Time spent writing a batch to databases",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
)

DB_WRITE_BATCHES = Counter(
    "consumer_db_write_batches_total",
    "Count of database write batches by target and status",
    labelnames=["target", "status"],
)

DB_RECONNECTIONS = Counter(
    "consumer_db_reconnections_total",
    "Database reconnection attempts",
    labelnames=["target"],
)


# ----------------------
# HTTP health/metrics server
# ----------------------
class HealthMetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path == "/health":
            self._send_health()
            return
        if self.path == "/metrics":
            self._send_metrics()
            return

        self.send_response(404)
        self.end_headers()

    def _send_health(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

    def _send_metrics(self) -> None:
        output = generate_latest()
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.send_header("Content-Length", str(len(output)))
        self.end_headers()
        self.wfile.write(output)

    def log_message(self, format, *args):  # noqa: A003
        return


def start_status_server(port: int) -> HTTPServer:
    server = HTTPServer(("0.0.0.0", port), HealthMetricsHandler)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ----------------------
# Database helpers
# ----------------------
class DualWriter:
    def __init__(self, cockroach_dsn: str, postgres_dsn: Optional[str]) -> None:
        self.cockroach_dsn = cockroach_dsn
        self.postgres_dsn = postgres_dsn
        self.cockroach_conn = None
        self.postgres_conn = None

    def _connect_with_retry(self, dsn: str, target: str):
        delay = 1
        while True:
            try:
                return self._connect(dsn, target)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Failed to connect to %s: %s. Retrying in %s seconds",
                    target,
                    exc,
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, 30)

    def _connect(self, dsn: str, target: str):
        DB_RECONNECTIONS.labels(target=target).inc()
        return psycopg2.connect(dsn, connect_timeout=5)

    def ensure_connections(self) -> None:
        if self.cockroach_conn is None or self.cockroach_conn.closed:
            LOGGER.info("Connecting to CockroachDB ...")
            self.cockroach_conn = self._connect_with_retry(
                self.cockroach_dsn, "cockroach"
            )
        if self.postgres_dsn:
            if self.postgres_conn is None or self.postgres_conn.closed:
                LOGGER.info("Connecting to Postgres for dual writes ...")
                self.postgres_conn = self._connect_with_retry(
                    self.postgres_dsn, "postgres"
                )

    @contextmanager
    def _cursor(self, use_postgres: bool):
        conn = self.postgres_conn if use_postgres else self.cockroach_conn
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _write_trades(self, cur, rows):
        execute_batch(
            cur,
            """
            INSERT INTO trades_raw (
              vendor, symbol, trade_id, event_time_ms, price, qty, is_buyer_maker
            ) VALUES (%(vendor)s, %(symbol)s, %(trade_id)s, %(event_time_ms)s, %(price)s, %(qty)s, %(is_buyer_maker)s)
            ON CONFLICT (vendor, symbol, event_ts, trade_id) DO NOTHING;
            """,
            rows,
        )

    def _write_book(self, cur, rows):
        execute_batch(
            cur,
            """
            INSERT INTO book_ticker_raw (
              vendor, symbol, update_id, event_time_ms, bid_price, bid_qty, ask_price, ask_qty
            ) VALUES (%(vendor)s, %(symbol)s, %(update_id)s, %(event_time_ms)s, %(bid_price)s, %(bid_qty)s, %(ask_price)s, %(ask_qty)s)
            ON CONFLICT (vendor, symbol, event_ts, update_id) DO NOTHING;
            """,
            rows,
        )
        execute_batch(
            cur,
            """
            INSERT INTO book_ticker_latest (
              vendor, symbol, event_ts, bid_price, bid_qty, ask_price, ask_qty
            ) VALUES (%(vendor)s, %(symbol)s, to_timestamp(%(event_time_ms)s::float8 / 1000.0), %(bid_price)s, %(bid_qty)s, %(ask_price)s, %(ask_qty)s)
            ON CONFLICT (vendor, symbol) DO UPDATE SET
              event_ts  = EXCLUDED.event_ts,
              bid_price = EXCLUDED.bid_price,
              bid_qty   = EXCLUDED.bid_qty,
              ask_price = EXCLUDED.ask_price,
              ask_qty   = EXCLUDED.ask_qty
            WHERE book_ticker_latest.event_ts <= EXCLUDED.event_ts;
            """,
            rows,
        )

    def _reset_connections(self) -> None:
        if self.cockroach_conn:
            try:
                self.cockroach_conn.close()
            except Exception:  # noqa: BLE001
                pass
        if self.postgres_conn:
            try:
                self.postgres_conn.close()
            except Exception:  # noqa: BLE001
                pass
        self.cockroach_conn = None
        self.postgres_conn = None

    def write_rows(self, rows, is_trade: bool) -> None:
        delay = 1
        while True:
            try:
                self.ensure_connections()
                writers = [(self.cockroach_conn, False)]
                if self.postgres_conn:
                    writers.append((self.postgres_conn, True))

                start = time.monotonic()
                try:
                    for _conn, use_postgres in writers:
                        target = "postgres" if use_postgres else "cockroach"
                        try:
                            with self._cursor(use_postgres) as cur:
                                if is_trade:
                                    self._write_trades(cur, rows)
                                else:
                                    self._write_book(cur, rows)
                            DB_WRITE_BATCHES.labels(
                                target=target, status="success"
                            ).inc()
                        except Exception:
                            DB_WRITE_BATCHES.labels(
                                target=target, status="failure"
                            ).inc()
                            raise
                finally:
                    BATCH_DURATION.observe(time.monotonic() - start)
                break
            except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
                LOGGER.warning(
                    "Database write failed due to connection issue: %s. Retrying in %s seconds",
                    exc,
                    delay,
                )
                self._reset_connections()
                time.sleep(delay)
                delay = min(delay * 2, 30)


# ----------------------
# Kafka consumer loop
# ----------------------


def _safe_int_field(msg: Dict, key: str, context: str) -> Optional[int]:
    value = msg.get(key)
    if value is None:
        LOGGER.error("Skipping %s: %s is None in message: %s", context, key, msg)
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        LOGGER.error(
            "Skipping %s: could not cast %s=%r to int in message: %s",
            context,
            key,
            value,
            msg,
        )
        return None


def parse_trade(msg: Dict) -> Optional[Dict]:
    context = "trade message"

    event_time_ms = _safe_int_field(msg, "event_time", context)
    trade_id = _safe_int_field(msg, "trade_id", context)
    if event_time_ms is None or trade_id is None:
        return None

    try:
        price = Decimal(str(msg.get("price")))
        qty = Decimal(str(msg.get("qty")))
    except (TypeError, ValueError, ArithmeticError) as exc:
        LOGGER.error("Skipping %s due to bad price/qty: %s | %s", context, msg, exc)
        return None

    return {
        "vendor": msg.get("vendor"),
        "symbol": msg.get("symbol"),
        "trade_id": trade_id,
        "event_time_ms": event_time_ms,
        "price": price,
        "qty": qty,
        "is_buyer_maker": bool(msg.get("is_buyer_maker")),
    }


def parse_book(msg: Dict) -> Optional[Dict]:
    context = "book message"

    event_time_ms = _safe_int_field(msg, "event_time", context)
    update_id = _safe_int_field(msg, "update_id", context)
    if event_time_ms is None or update_id is None:
        return None

    try:
        bid_price = Decimal(str(msg.get("bid_price")))
        bid_qty = Decimal(str(msg.get("bid_qty")))
        ask_price = Decimal(str(msg.get("ask_price")))
        ask_qty = Decimal(str(msg.get("ask_qty")))
    except (TypeError, ValueError, ArithmeticError) as exc:
        LOGGER.error("Skipping %s due to bad price/qty: %s | %s", context, msg, exc)
        return None

    return {
        "vendor": msg.get("vendor"),
        "symbol": msg.get("symbol"),
        "update_id": update_id,
        "event_time_ms": event_time_ms,
        "bid_price": bid_price,
        "bid_qty": bid_qty,
        "ask_price": ask_price,
        "ask_qty": ask_qty,
    }


def consume_topic():
    topic = os.getenv("KAFKA_TOPIC", "market.trades.raw")
    group_id = os.getenv("KAFKA_GROUP", "cockroach-consumer")
    cockroach_dsn = os.getenv(
        "COCKROACH_DSN",
        "postgresql://root@cockroachdb-node1:26257/market?sslmode=disable",
    )
    dual_write_enabled = os.getenv("ENABLE_DUAL_WRITE", "true").lower() == "true"
    postgres_dsn = os.getenv("POSTGRES_DSN") if dual_write_enabled else None
    if dual_write_enabled and not postgres_dsn:
        raise ValueError("ENABLE_DUAL_WRITE is true but POSTGRES_DSN is not set")
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    poll_timeout = float(os.getenv("POLL_TIMEOUT_SEC", "1.0"))
    health_port = int(os.getenv("HEALTH_PORT", "8088"))

    LOGGER.info(
        "Starting consumer for topic=%s group=%s batch_size=%d",
        topic,
        group_id,
        batch_size,
    )
    LOGGER.info("Dual-write to Postgres: %s", bool(postgres_dsn))

    writer = DualWriter(cockroach_dsn, postgres_dsn)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[
            h.strip()
            for h in os.getenv("KAFKA_BROKER", "kafka:9092").split(",")
            if h.strip()
        ],
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=os.getenv("AUTO_OFFSET_RESET", "earliest"),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    start_status_server(health_port)

    stop_event = threading.Event()

    def handle_signal(signum, frame):  # noqa: ANN001, D401
        LOGGER.info("Received signal %s; shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    buffer = []
    is_trade_topic = topic == "market.trades.raw"

    try:
        while not stop_event.is_set():
            msg_pack = consumer.poll(timeout_ms=int(poll_timeout * 1000))
            for _tp, messages in msg_pack.items():
                for record in messages:
                    try:
                        parsed = (
                            parse_trade(record.value)
                            if is_trade_topic
                            else parse_book(record.value)
                        )
                        if parsed is None:
                            # Already logged as malformed / bad
                            continue

                        buffer.append(parsed)
                        MESSAGES_CONSUMED.labels(topic=topic).inc()
                    except Exception:
                        LOGGER.exception(
                            "Unexpected error while parsing message, skipping: %s",
                            record.value,
                        )
                        continue

                    if len(buffer) >= batch_size:
                        writer.write_rows(buffer, is_trade=is_trade_topic)
                        consumer.commit()
                        buffer.clear()

            if stop_event.is_set():
                break

        if buffer:
            writer.write_rows(buffer, is_trade=is_trade_topic)
            consumer.commit()
    finally:
        consumer.close()
        LOGGER.info("Consumer stopped")


if __name__ == "__main__":
    consume_topic()
