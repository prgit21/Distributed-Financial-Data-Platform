#!/usr/bin/env bash
set -euo pipefail

PG_SERVICE="postgres"
PG_DB="market"
PG_USER="postgres"

echo "=== Partitioning trades_raw by event_time_ms (Postgres) ==="
echo

psqlc() {
  docker compose exec -T "${PG_SERVICE}" \
    psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 "$@"
}

echo "-- Current definition of trades_raw (before migration):"
psqlc -c '\d trades_raw' || {
  echo "ERROR: trades_raw does not exist. Aborting."
  exit 1
}

echo
echo ">>> Migrating trades_raw -> partitioned trades_raw (RANGE on event_time_ms)..."
echo "    This will: rename trades_raw, create a partitioned parent, create partitions,"
echo "    copy data, drop old table, and recreate indexes."
echo

psqlc <<'SQL'
BEGIN;

-- 1) Rename existing table out of the way
ALTER TABLE trades_raw RENAME TO trades_raw_base;

-- 2) Create new partitioned parent table
--    partition key = event_time_ms
--    PK includes event_time_ms (required by Postgres for partitioned tables)
CREATE TABLE trades_raw (
  vendor         text                     NOT NULL,
  symbol         text                     NOT NULL,
  trade_id       bigint                   NOT NULL,
  event_time_ms  bigint                   NOT NULL,
  event_ts       timestamptz              GENERATED ALWAYS AS (
                     to_timestamp(event_time_ms::double precision / 1000.0::double precision)
                   ) STORED,
  price          numeric                  NOT NULL,
  qty            numeric                  NOT NULL,
  is_buyer_maker boolean                  NOT NULL,
  PRIMARY KEY (vendor, symbol, event_time_ms, trade_id)
) PARTITION BY RANGE (event_time_ms);

-- 3) Create range partitions by event_time_ms (epoch ms)
--    2024-01-01 UTC => 1704067200000
--    2025-01-01 UTC => 1735689600000
CREATE TABLE trades_raw_p_early PARTITION OF trades_raw
  FOR VALUES FROM (MINVALUE) TO (1704067200000);

CREATE TABLE trades_raw_p_2024 PARTITION OF trades_raw
  FOR VALUES FROM (1704067200000) TO (1735689600000);

CREATE TABLE trades_raw_p_2025_onward PARTITION OF trades_raw
  FOR VALUES FROM (1735689600000) TO (MAXVALUE);

-- 4) Copy existing data into the new partitioned table
--    IMPORTANT: do NOT insert into generated column event_ts.
INSERT INTO trades_raw (
  vendor, symbol, trade_id, event_time_ms, price, qty, is_buyer_maker
)
SELECT
  vendor, symbol, trade_id, event_time_ms, price, qty, is_buyer_maker
FROM trades_raw_base;

-- 5) Drop the old base table
DROP TABLE trades_raw_base;

-- 6) Recreate useful indexes on parent (become partitioned indexes)
CREATE INDEX idx_trades_event_ts
  ON trades_raw(event_ts);

CREATE INDEX idx_trades_symbol_event_ts
  ON trades_raw(symbol, event_ts);

COMMIT;
SQL

echo
echo "=== New definition of trades_raw (partitioned) ==="
psqlc -c '\d+ trades_raw'

echo
echo "=== List of partitions for trades_raw ==="
psqlc -c "
SELECT
  inhrelid::regclass AS partition_name,
  pg_get_expr(pg_class.relpartbound, pg_class.oid, true) AS partition_bound
FROM pg_inherits
JOIN pg_class ON pg_class.oid = inhrelid
JOIN pg_class parent ON parent.oid = inhparent
WHERE parent.relname = 'trades_raw'
ORDER BY partition_name;
"

echo
echo "=== Quick partitioning sanity: approx row counts per partition ==="
psqlc -c "
SELECT
  inhrelid::regclass AS partition_name,
  reltuples::bigint AS approx_rows
FROM pg_inherits
JOIN pg_class ON pg_class.oid = inhrelid
JOIN pg_class parent ON parent.oid = inhparent
WHERE parent.relname = 'trades_raw'
ORDER BY partition_name;
"

echo
echo "=== EXPLAIN ANALYZE: time-window query on partitioned trades_raw ==="
psqlc -c "
EXPLAIN ANALYZE
SELECT *
FROM trades_raw
WHERE event_ts >= now() - interval '10 minutes'
LIMIT 200;
"

echo
echo ">>> Done. trades_raw is now RANGE-partitioned on event_time_ms."
echo "    Table name and columns stay the same; existing queries should keep working."
echo "    You can re-run ./test_postgres_sanity.sh to reconfirm consistency & durability."
