CREATE DATABASE IF NOT EXISTS market;
\c market;

-- Vendors dimension
CREATE TABLE IF NOT EXISTS vendors (
    vendor TEXT PRIMARY KEY
);

-- Crypto Symbols
CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    base_asset TEXT,
    quote_asset TEXT,
    tick_size DECIMAL(38,18),
    lot_size DECIMAL(38,18)
);

INSERT INTO vendors (vendor)
VALUES ('binance')
ON CONFLICT (vendor) DO NOTHING;

-- Raw Trades
CREATE TABLE IF NOT EXISTS trades_raw (
    vendor TEXT NOT NULL REFERENCES vendors(vendor),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    trade_id BIGINT NOT NULL,
    event_time_ms BIGINT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (
        to_timestamp(event_time_ms::double precision / 1000.0)
    ) STORED,
    price DECIMAL(38,18) NOT NULL,
    qty DECIMAL(38,18) NOT NULL,
    is_buyer_maker BOOL NOT NULL,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor, symbol, event_ts, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
    ON trades_raw (symbol, event_ts DESC);

ALTER TABLE trades_raw
    ADD CONSTRAINT chk_trades_price CHECK (price > 0),
    ADD CONSTRAINT chk_trades_qty CHECK (qty > 0),
    ADD CONSTRAINT chk_trades_event_ms CHECK (event_time_ms > 0);

-- Order Book Top-of-Book Raw Updates
CREATE TABLE IF NOT EXISTS book_ticker_raw (
    vendor TEXT NOT NULL REFERENCES vendors(vendor),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    update_id BIGINT NOT NULL,
    event_time_ms BIGINT NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL GENERATED ALWAYS AS (
        to_timestamp(event_time_ms::double precision / 1000.0)
    ) STORED,
    bid_price DECIMAL(38,18) NOT NULL,
    bid_qty DECIMAL(38,18) NOT NULL,
    ask_price DECIMAL(38,18) NOT NULL,
    ask_qty DECIMAL(38,18) NOT NULL,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor, symbol, event_ts, update_id)
);

CREATE INDEX IF NOT EXISTS idx_book_symbol_ts
    ON book_ticker_raw (symbol, event_ts DESC);

ALTER TABLE book_ticker_raw
    ADD CONSTRAINT chk_book_bid CHECK (bid_price > 0 AND bid_qty >= 0),
    ADD CONSTRAINT chk_book_ask CHECK (ask_price > 0 AND ask_qty >= 0),
    ADD CONSTRAINT chk_book_event CHECK (event_time_ms > 0);

-- Latest Best Bid/Ask Snapshot
CREATE TABLE IF NOT EXISTS book_ticker_latest (
    vendor TEXT NOT NULL REFERENCES vendors(vendor),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    event_ts TIMESTAMPTZ NOT NULL,
    bid_price DECIMAL(38,18) NOT NULL,
    bid_qty DECIMAL(38,18) NOT NULL,
    ask_price DECIMAL(38,18) NOT NULL,
    ask_qty DECIMAL(38,18) NOT NULL,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vendor, symbol)
);

-- Aggregated 1-minute OHLCV bars
CREATE TABLE IF NOT EXISTS trades_1m_bar (
    vendor TEXT NOT NULL REFERENCES vendors(vendor),
    symbol TEXT NOT NULL REFERENCES symbols(symbol),
    bucket_minute TIMESTAMPTZ NOT NULL,
    open_price DECIMAL(38,18) NOT NULL,
    high_price DECIMAL(38,18) NOT NULL,
    low_price DECIMAL(38,18) NOT NULL,
    close_price DECIMAL(38,18) NOT NULL,
    volume DECIMAL(38,18) NOT NULL,
    PRIMARY KEY (vendor, symbol, bucket_minute)
);
