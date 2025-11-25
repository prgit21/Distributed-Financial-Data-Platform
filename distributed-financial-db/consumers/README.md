# Kafka -> CockroachDB / Postgres consumers

These services consume the same normalized payloads published by the Binance websocket ingestor (`ingestion/worker.py`) and upsert them into the CockroachDB tables defined in `db/schema_cockroach.sql`. The payload field names map directly to the schema columns:

- `market.trades.raw` messages carry `vendor`, `symbol`, `trade_id`, `event_time` (ms), `price`, `qty`, and `is_buyer_maker`, which align with `trades_raw.vendor`, `symbol`, `trade_id`, `event_time_ms`, `price`, `qty`, and `is_buyer_maker`.
- `market.book_ticker.raw` messages contain `vendor`, `symbol`, `update_id`, `event_time` (ms), `bid_price`, `bid_qty`, `ask_price`, and `ask_qty`, matching `book_ticker_raw` as well as the `book_ticker_latest` snapshot table.

Each consumer performs idempotent `INSERT ... ON CONFLICT` writes to handle replayed Kafka offsets safely, commits offsets only after successful database transactions, and can optionally dual-write to Postgres (using the same schema) for benchmarking.

## Validation plan for dual writes

1. **Seed schemas**: apply `db/schema_cockroach.sql` to CockroachDB and mirror the same tables in Postgres (the schema is compatible as-is for these tables).
2. **Start the stack with dual writes enabled**: `ENABLE_DUAL_WRITE=true docker compose up --build trades-consumer book-consumer postgres cockroachdb-node1 cockroachdb-node2 cockroachdb-node3 kafka zookeeper`.
3. **Produce sample events**: publish a few trade and book messages matching the documented payload shape to `market.trades.raw` and `market.book_ticker.raw` (e.g., via `kcat` or the ingestion worker).
4. **Verify CockroachDB writes**: run `SELECT COUNT(*) FROM trades_raw;` and `SELECT COUNT(*) FROM book_ticker_raw;` in CockroachDB to ensure rows arrived and constraints (positive price/qty) hold.
5. **Verify Postgres parity**: run the same counts in Postgres and ensure they match CockroachDB. Spot-check latest snapshot rows in `book_ticker_latest` for correct bid/ask updates.
6. **Idempotency check**: re-publish the exact same messages and confirm counts stay unchanged (conflict target `(vendor, symbol, event_ts, trade_id/update_id)` prevents duplicates).
7. **Offset safety**: force a consumer restart (e.g., `docker compose restart trades-consumer`) and ensure no duplicate rows appear and offsets resume correctly after the last committed transaction.
