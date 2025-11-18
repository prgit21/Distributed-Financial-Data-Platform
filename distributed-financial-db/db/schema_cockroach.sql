-- CockroachDB schema for Distributed Financial Data Platform
-- Time-series optimized, FK-safe, with TTL and aggregation table

CREATE DATABASE IF NOT EXISTS market;
SET DATABASE = market;

-- ========== Dimension tables ==========

CREATE TABLE IF NOT EXISTS vendors (
  vendor TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS symbols (
  symbol       TEXT PRIMARY KEY,
  base_asset   TEXT,
  quote_asset  TEXT,
  tick_size    DECIMAL(38,18),
  lot_size     DECIMAL(38,18)
);

-- Seed default vendor (idempotent)
INSERT INTO vendors (vendor) VALUES ('binance')
ON CONFLICT (vendor) DO NOTHING;

-- ========== market.trades.raw (per-trade ticks) ==========

CREATE TABLE IF NOT EXISTS trades_raw (
  vendor          TEXT NOT NULL REFERENCES vendors (vendor),
  symbol          TEXT NOT NULL REFERENCES symbols (symbol),
  trade_id        BIGINT NOT NULL,
  event_time_ms   BIGINT NOT NULL,
  event_ts        TIMESTAMPTZ NOT NULL AS (
                     to_timestamp(event_time_ms::FLOAT8 / 1000.0)
                   ) STORED,
  price           DECIMAL(38,18) NOT NULL,
  qty             DECIMAL(38,18) NOT NULL,
  is_buyer_maker  BOOL NOT NULL,
  ingest_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (vendor, symbol, event_ts, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
  ON trades_raw (symbol, event_ts DESC);

ALTER TABLE trades_raw
  ADD CONSTRAINT IF NOT EXISTS chk_trades_price_positive CHECK (price > 0),
  ADD CONSTRAINT IF NOT EXISTS chk_trades_qty_positive   CHECK (qty > 0),
  ADD CONSTRAINT IF NOT EXISTS chk_trades_event_time_ms  CHECK (event_time_ms > 0);

-- ========== market.book_ticker.raw (top-of-book updates) ==========

CREATE TABLE IF NOT EXISTS book_ticker_raw (
  vendor          TEXT NOT NULL REFERENCES vendors (vendor),
  symbol          TEXT NOT NULL REFERENCES symbols (symbol),
  update_id       BIGINT NOT NULL,
  event_time_ms   BIGINT NOT NULL,
  event_ts        TIMESTAMPTZ NOT NULL AS (
                     to_timestamp(event_time_ms::FLOAT8 / 1000.0)
                   ) STORED,
  bid_price       DECIMAL(38,18) NOT NULL,
  bid_qty         DECIMAL(38,18) NOT NULL,
  ask_price       DECIMAL(38,18) NOT NULL,
  ask_qty         DECIMAL(38,18) NOT NULL,
  ingest_ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (vendor, symbol, event_ts, update_id)
);

CREATE INDEX IF NOT EXISTS idx_book_symbol_ts
  ON book_ticker_raw (symbol, event_ts DESC);

ALTER TABLE book_ticker_raw
  ADD CONSTRAINT IF NOT EXISTS chk_book_bid_positive CHECK (bid_price > 0 AND bid_qty >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_book_ask_positive CHECK (ask_price > 0 AND ask_qty >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_book_event_ms     CHECK (event_time_ms > 0);

-- ========== Snapshot for current best bid/ask per (vendor, symbol) ==========

CREATE TABLE IF NOT EXISTS book_ticker_latest (
  vendor     TEXT NOT NULL REFERENCES vendors (vendor),
  symbol     TEXT NOT NULL REFERENCES symbols (symbol),
  event_ts   TIMESTAMPTZ NOT NULL,
  bid_price  DECIMAL(38,18) NOT NULL,
  bid_qty    DECIMAL(38,18) NOT NULL,
  ask_price  DECIMAL(38,18) NOT NULL,
  ask_qty    DECIMAL(38,18) NOT NULL,
  ingest_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (vendor, symbol)
);

-- ========== Aggregated 1-minute OHLCV bars ==========

CREATE TABLE IF NOT EXISTS trades_1m_bar (
  vendor         TEXT NOT NULL REFERENCES vendors (vendor),
  symbol         TEXT NOT NULL REFERENCES symbols (symbol),
  bucket_minute  TIMESTAMPTZ NOT NULL,
  open_price     DECIMAL(38,18) NOT NULL,
  high_price     DECIMAL(38,18) NOT NULL,
  low_price      DECIMAL(38,18) NOT NULL,
  close_price    DECIMAL(38,18) NOT NULL,
  volume         DECIMAL(38,18) NOT NULL,
  PRIMARY KEY (vendor, symbol, bucket_minute)
);

-- ========== TTL on raw tables (keep last 7 days) ==========

ALTER TABLE trades_raw
  SET (ttl_expiration_expression = 'event_ts + INTERVAL ''168 hours''',
       ttl_job_cron = '@hourly');

ALTER TABLE book_ticker_raw
  SET (ttl_expiration_expression = 'event_ts + INTERVAL ''168 hours''',
       ttl_job_cron = '@hourly');
