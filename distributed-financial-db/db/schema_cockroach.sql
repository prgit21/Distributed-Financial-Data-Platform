-- db/schema_cockroach.sql
-- Assumes we are already connected to the "market" database.

CREATE TABLE IF NOT EXISTS trades_raw (
  vendor        STRING      NOT NULL,
  symbol        STRING      NOT NULL,
  trade_id      INT8        NOT NULL,
  event_time_ms INT8        NOT NULL,
  event_ts      TIMESTAMPTZ AS (to_timestamp(event_time_ms::FLOAT8 / 1000.0)) STORED,
  price         DECIMAL     NOT NULL,
  qty           DECIMAL     NOT NULL,
  is_buyer_maker BOOL       NOT NULL,
  CONSTRAINT pk_trades_raw PRIMARY KEY (vendor, symbol, event_ts, trade_id)
);

CREATE TABLE IF NOT EXISTS book_ticker_raw (
  vendor        STRING      NOT NULL,
  symbol        STRING      NOT NULL,
  update_id     INT8        NOT NULL,
  event_time_ms INT8        NOT NULL,
  event_ts      TIMESTAMPTZ AS (to_timestamp(event_time_ms::FLOAT8 / 1000.0)) STORED,
  bid_price     DECIMAL     NOT NULL,
  bid_qty       DECIMAL     NOT NULL,
  ask_price     DECIMAL     NOT NULL,
  ask_qty       DECIMAL     NOT NULL,
  CONSTRAINT pk_book_ticker_raw PRIMARY KEY (vendor, symbol, event_ts, update_id)
);

CREATE TABLE IF NOT EXISTS book_ticker_latest (
  vendor    STRING      NOT NULL,
  symbol    STRING      NOT NULL,
  event_ts  TIMESTAMPTZ NOT NULL,
  bid_price DECIMAL     NOT NULL,
  bid_qty   DECIMAL     NOT NULL,
  ask_price DECIMAL     NOT NULL,
  ask_qty   DECIMAL     NOT NULL,
  CONSTRAINT pk_book_ticker_latest PRIMARY KEY (vendor, symbol)
);
