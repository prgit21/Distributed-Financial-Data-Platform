-- DB: market (same as CRDB)
-- The container below creates the database automatically (POSTGRES_DB=market).

-- Dimension tables
CREATE TABLE IF NOT EXISTS vendors (
  vendor TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS symbols (
  symbol TEXT PRIMARY KEY
);

INSERT INTO vendors (vendor) VALUES ('binance')
ON CONFLICT (vendor) DO NOTHING;

-- Raw trades
CREATE TABLE IF NOT EXISTS trades_raw (
  vendor          TEXT NOT NULL REFERENCES vendors (vendor),
  symbol          TEXT NOT NULL REFERENCES symbols (symbol),
  trade_id        BIGINT NOT NULL,
  event_time_ms   BIGINT NOT NULL,
  event_ts        TIMESTAMPTZ NOT NULL,
  price           NUMERIC(38,18) NOT NULL,
  qty             NUMERIC(38,18) NOT NULL,
  is_buyer_maker  BOOLEAN NOT NULL,
  PRIMARY KEY (vendor, symbol, trade_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
  ON trades_raw (symbol, event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_trades_vendor_symbol_ts
  ON trades_raw (vendor, symbol, event_ts DESC);

-- Raw book ticker
CREATE TABLE IF NOT EXISTS book_ticker_raw (
  vendor          TEXT NOT NULL REFERENCES vendors (vendor),
  symbol          TEXT NOT NULL REFERENCES symbols (symbol),
  update_id       BIGINT NOT NULL,
  event_time_ms   BIGINT NOT NULL,
  event_ts        TIMESTAMPTZ NOT NULL,
  bid_price       NUMERIC(38,18) NOT NULL,
  bid_qty         NUMERIC(38,18) NOT NULL,
  ask_price       NUMERIC(38,18) NOT NULL,
  ask_qty         NUMERIC(38,18) NOT NULL,
  PRIMARY KEY (vendor, symbol, event_time_ms, update_id)
);
CREATE INDEX IF NOT EXISTS idx_book_symbol_ts
  ON book_ticker_raw (symbol, event_ts DESC);

-- Latest best bid/ask per (vendor, symbol)
CREATE TABLE IF NOT EXISTS book_ticker_latest (
  vendor     TEXT NOT NULL REFERENCES vendors (vendor),
  symbol     TEXT NOT NULL REFERENCES symbols (symbol),
  event_ts   TIMESTAMPTZ NOT NULL,
  bid_price  NUMERIC(38,18) NOT NULL,
  bid_qty    NUMERIC(38,18) NOT NULL,
  ask_price  NUMERIC(38,18) NOT NULL,
  ask_qty    NUMERIC(38,18) NOT NULL,
  PRIMARY KEY (vendor, symbol)
);