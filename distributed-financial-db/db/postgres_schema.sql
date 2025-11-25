-- db/postgres_schema.sql
-- Runs inside the already-created POSTGRES_DB=market.
-- Do NOT create database or \connect here.

CREATE TABLE IF NOT EXISTS trades_raw (
  vendor         TEXT    NOT NULL,
  symbol         TEXT    NOT NULL,
  trade_id       BIGINT  NOT NULL,
  event_time_ms  BIGINT  NOT NULL,
  event_ts       TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(event_time_ms::float8 / 1000.0)) STORED,
  price          NUMERIC NOT NULL,
  qty            NUMERIC NOT NULL,
  is_buyer_maker BOOLEAN NOT NULL,
  CONSTRAINT pk_trades_raw PRIMARY KEY (vendor, symbol, event_ts, trade_id)
);

CREATE TABLE IF NOT EXISTS book_ticker_raw (
  vendor        TEXT    NOT NULL,
  symbol        TEXT    NOT NULL,
  update_id     BIGINT  NOT NULL,
  event_time_ms BIGINT  NOT NULL,
  event_ts      TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(event_time_ms::float8 / 1000.0)) STORED,
  bid_price     NUMERIC NOT NULL,
  bid_qty       NUMERIC NOT NULL,
  ask_price     NUMERIC NOT NULL,
  ask_qty       NUMERIC NOT NULL,
  CONSTRAINT pk_book_ticker_raw PRIMARY KEY (vendor, symbol, event_ts, update_id)
);

CREATE TABLE IF NOT EXISTS book_ticker_latest (
  vendor    TEXT        NOT NULL,
  symbol    TEXT        NOT NULL,
  event_ts  TIMESTAMPTZ NOT NULL,
  bid_price NUMERIC     NOT NULL,
  bid_qty   NUMERIC     NOT NULL,
  ask_price NUMERIC     NOT NULL,
  ask_qty   NUMERIC     NOT NULL,
  CONSTRAINT pk_book_ticker_latest PRIMARY KEY (vendor, symbol)
);
