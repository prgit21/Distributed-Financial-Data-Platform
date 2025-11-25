import os, json, time, signal
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Tuple, Dict
from confluent_kafka import Consumer, KafkaError
import psycopg
from psycopg_pool import ConnectionPool

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TRADES_TOPIC      = os.getenv("KAFKA_TOPIC_TRADES", "binance.trades")
BOOK_TOPIC        = os.getenv("KAFKA_TOPIC_BOOK", "binance.book_ticker")
GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "pg-sink-binance")
PG_DSN            = os.getenv("PG_DSN", "postgresql://postgres:postgres@postgres:5432/market")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "1000"))
FLUSH_MS          = int(os.getenv("FLUSH_MS", "1000"))

SQL_INS_VENDOR = "INSERT INTO vendors(vendor) VALUES (%s) ON CONFLICT DO NOTHING"
SQL_INS_SYMBOL = "INSERT INTO symbols(symbol) VALUES (%s) ON CONFLICT DO NOTHING"

SQL_INS_TRADES = """
INSERT INTO trades_raw
(vendor, symbol, trade_id, event_time_ms, event_ts, price, qty, is_buyer_maker)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (vendor, symbol, trade_id) DO NOTHING
"""

SQL_INS_BOOK_RAW = """
INSERT INTO book_ticker_raw
(vendor, symbol, update_id, event_time_ms, event_ts, bid_price, bid_qty, ask_price, ask_qty)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (vendor, symbol, event_time_ms, update_id) DO NOTHING
"""

SQL_UPSERT_BOOK_LATEST = """
INSERT INTO book_ticker_latest
(vendor, symbol, event_ts, bid_price, bid_qty, ask_price, ask_qty)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (vendor, symbol) DO UPDATE
SET event_ts = EXCLUDED.event_ts,
    bid_price = EXCLUDED.bid_price,
    bid_qty   = EXCLUDED.bid_qty,
    ask_price = EXCLUDED.ask_price,
    ask_qty   = EXCLUDED.ask_qty
WHERE EXCLUDED.event_ts > book_ticker_latest.event_ts
"""

def ts_from_ms(ms): return datetime.fromtimestamp(int(ms)/1000.0, tz=timezone.utc)
def D(x): return None if x is None else Decimal(str(x))

def parse_trade(d):
    vendor = d.get("vendor") or "binance"
    symbol = (d.get("symbol") or d.get("s") or "").upper()
    trade_id = d.get("trade_id", d.get("t"))
    evt_ms = d.get("event_time_ms", d.get("E") or d.get("T"))
    price = d.get("price", d.get("p"))
    qty   = d.get("qty",   d.get("q"))
    maker = d.get("is_buyer_maker", d.get("m"))
    if not vendor or not symbol or trade_id is None or evt_ms is None:
        return None, None, None
    row = (vendor, symbol, int(trade_id), int(evt_ms), ts_from_ms(evt_ms), D(price), D(qty), bool(maker))
    return row, vendor, symbol

def parse_book(d):
    vendor = d.get("vendor") or "binance"
    symbol = (d.get("symbol") or d.get("s") or "").upper()
    update_id = d.get("update_id", d.get("u"))
    evt_ms = d.get("event_time_ms", d.get("E") or d.get("eventTime"))
    bid_p = d.get("bid_price", d.get("b")); bid_q = d.get("bid_qty", d.get("B"))
    ask_p = d.get("ask_price", d.get("a")); ask_q = d.get("ask_qty", d.get("A"))
    if not vendor or not symbol or update_id is None or evt_ms is None:
        return None, None, None, None
    event_ts = ts_from_ms(evt_ms)
    raw = (vendor, symbol, int(update_id), int(evt_ms), event_ts, D(bid_p), D(bid_q), D(ask_p), D(ask_q))
    latest = (vendor, symbol, event_ts, D(bid_p), D(bid_q), D(ask_p), D(ask_q))
    return raw, latest, vendor, symbol

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 600000
    })
    consumer.subscribe([TRADES_TOPIC, BOOK_TOPIC])

    pool = ConnectionPool(PG_DSN, min_size=1, max_size=4, open=True)

    trades: List[Tuple] = []
    book_raw: List[Tuple] = []
    book_latest: List[Tuple] = []
    vendors: set[str] = set()
    symbols: set[str] = set()

    last_by_partition = {}
    last_flush = time.time()
    running = True
    def stop(*_): 
        nonlocal running; running = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f"[pg-sink] consuming {[TRADES_TOPIC, BOOK_TOPIC]} -> {PG_DSN}", flush=True)

    while running:
        msg = consumer.poll(0.1)
        now = time.time()

        if msg is None:
            if (now - last_flush) * 1000 >= FLUSH_MS:
                flush(pool, consumer, trades, book_raw, book_latest, vendors, symbols, last_by_partition)
                trades.clear(); book_raw.clear(); book_latest.clear()
                vendors.clear(); symbols.clear(); last_by_partition.clear()
                last_flush = now
            continue

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[pg-sink] Kafka error: {msg.error()}", flush=True)
            continue

        try:
            d = json.loads(msg.value())
            t = msg.topic()
            if t == TRADES_TOPIC:
                row, v, s = parse_trade(d)
                if row: trades.append(row); vendors.add(v); symbols.add(s)
            elif t == BOOK_TOPIC:
                raw, latest, v, s = parse_book(d)
                if raw:
                    book_raw.append(raw); book_latest.append(latest)
                    vendors.add(v); symbols.add(s)

            last_by_partition[(msg.topic(), msg.partition())] = msg

            if (len(trades) + len(book_raw)) >= BATCH_SIZE or ((now - last_flush) * 1000 >= FLUSH_MS):
                flush(pool, consumer, trades, book_raw, book_latest, vendors, symbols, last_by_partition)
                trades.clear(); book_raw.clear(); book_latest.clear()
                vendors.clear(); symbols.clear(); last_by_partition.clear()
                last_flush = now

        except Exception as e:
            print(f"[pg-sink] skip malformed: {e} payload={msg.value()!r}", flush=True)

    flush(pool, consumer, trades, book_raw, book_latest, vendors, symbols, last_by_partition)
    consumer.close(); pool.close()

def flush(pool, consumer, trades, book_raw, book_latest, vendors, symbols, last_by_partition):
    if not trades and not book_raw and not book_latest and not vendors and not symbols:
        return
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                if vendors:
                    cur.executemany(SQL_INS_VENDOR, [(v,) for v in sorted(vendors)])
                if symbols:
                    cur.executemany(SQL_INS_SYMBOL, [(s,) for s in sorted(symbols)])
                if trades:
                    cur.executemany(SQL_INS_TRADES, trades)
                if book_raw:
                    cur.executemany(SQL_INS_BOOK_RAW, book_raw)
                if book_latest:
                    cur.executemany(SQL_UPSERT_BOOK_LATEST, book_latest)
            conn.commit()

        for m in last_by_partition.values():  # commit offsets AFTER DB commit
            consumer.commit(message=m, asynchronous=False)
        print(f"[pg-sink] wrote trades={len(trades)} book_raw={len(book_raw)} latest={len(book_latest)}; offsets committed", flush=True)

    except Exception as e:
        print(f"[pg-sink] DB write failed; retrying (no offsets committed): {e}", flush=True)
        time.sleep(1.0)

if __name__ == "__main__":
    main()