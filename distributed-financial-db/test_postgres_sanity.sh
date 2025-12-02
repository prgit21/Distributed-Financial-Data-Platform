#!/usr/bin/env bash
set -euo pipefail

PG_SERVICE="postgres"
PG_DB="market"
PG_USER="postgres"

echo "=== Postgres Sanity Test: Partitioning, 'Sharding', Consistency ==="
echo

# Small helper to run psql inside the container
psqlc() {
  docker compose exec -T "${PG_SERVICE}" \
    psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 "$@"
}

echo "=== 1) Partitioning check ==="
echo

echo "-- Checking for any partitioned tables in database '${PG_DB}'..."
psqlc -c "
SELECT c.relname AS table_name
FROM pg_partitioned_table pt
JOIN pg_class c ON c.oid = pt.partrelid
ORDER BY c.relname;
"

echo
echo "-- Describing trades_raw to confirm it is not partitioned..."
psqlc -c '\d+ trades_raw'

echo
echo ">>> Result: If the partitioned-tables query returned no rows, and trades_raw has no 'Partition key' section,"
echo ">>> then Postgres is running with a single non-partitioned trades_raw table (no built-in partitioning)."
echo

echo "=== 2) 'Sharding' check (single-node sanity) ==="
echo

echo "-- Checking docker compose status for Postgres..."
docker compose ps "${PG_SERVICE}" || true

echo
echo ">>> Result: You should see exactly one '${PG_SERVICE}' container."
echo ">>> There is no Postgres-level sharding in this setup; all data lives on this single instance."
echo

echo "=== 3) Consistency check: serializable isolation (no lost updates) ==="
echo

echo "-- Creating test table serial_test_pg..."
psqlc -c "
DROP TABLE IF EXISTS serial_test_pg;
CREATE TABLE serial_test_pg (
  id      INT PRIMARY KEY,
  balance INT NOT NULL
);
INSERT INTO serial_test_pg VALUES (1, 100);
SELECT * FROM serial_test_pg;
"

echo
echo "-- Running TWO concurrent SERIALIZABLE transactions that both update the same row."
echo "   Expectation: one transaction commits, the other gets a serialization error (or is forced to retry)."
echo

TMPDIR=$(mktemp -d)
TX1_LOG="${TMPDIR}/tx1.log"
TX2_LOG="${TMPDIR}/tx2.log"

echo "-- Starting Transaction 1 (SERIALIZABLE, -30, with pg_sleep to keep it open)..."
docker compose exec -T "${PG_SERVICE}" \
  psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 >"${TX1_LOG}" 2>&1 <<'SQL' &
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE serial_test_pg SET balance = balance - 30 WHERE id = 1;
SELECT pg_sleep(3);
COMMIT;
SQL
TX1_PID=$!

# Small delay so tx1 is in progress before tx2 starts
sleep 1

echo "-- Starting Transaction 2 (SERIALIZABLE, -50)..."
set +e
docker compose exec -T "${PG_SERVICE}" \
  psql -U "${PG_USER}" -d "${PG_DB}" -v ON_ERROR_STOP=1 >"${TX2_LOG}" 2>&1 <<'SQL'
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
UPDATE serial_test_pg SET balance = balance - 50 WHERE id = 1;
COMMIT;
SQL
TX2_RC=$?
set -e

echo "-- Waiting for Transaction 1 to finish..."
wait "${TX1_PID}" || true

echo
echo "-- Transaction 1 log:"
cat "${TX1_LOG}" || true
echo
echo "-- Transaction 2 log (exit code: ${TX2_RC}):"
cat "${TX2_LOG}" || true
echo

echo "-- Final state of serial_test_pg:"
psqlc -c "SELECT * FROM serial_test_pg;"

echo
echo ">>> Interpretation:"
echo "  - You should see exactly one committed change to balance (e.g., 70 or 50)."
echo "  - At least one transaction should show a serialization/retry error if the overlap was sufficient."
echo "  - In all cases, there should be NO 'lost update' (e.g., not 20 = 100 - 30 - 50)."
echo

echo "=== 4) Consistency/Durability check: committed data survives restart ==="
echo

echo "-- Inserting a committed row into serial_test_pg (id = 2)..."
psqlc -c "
INSERT INTO serial_test_pg (id, balance)
VALUES (2, 999)
ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance;
SELECT * FROM serial_test_pg WHERE id = 2;
"

echo
echo "-- Restarting Postgres container to test durability of committed data..."
docker compose restart "${PG_SERVICE}"

echo
echo "-- Verifying that committed row (id = 2, balance = 999) is still present..."
psqlc -c "SELECT * FROM serial_test_pg WHERE id = 2;"

echo
echo ">>> Result: If row (2, 999) is still there after restart, committed data is durable."
echo

echo "=== Postgres sanity tests complete. ==="
echo "Logs for concurrent tx test are in: ${TMPDIR}"
