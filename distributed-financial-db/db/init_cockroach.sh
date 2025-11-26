#!/bin/bash
set -e

echo "Waiting for CockroachDB node to be reachable..."
until /cockroach/cockroach sql --insecure --host=cockroachdb-node1 -e 'SELECT 1'; do
  echo "CockroachDB not ready yet... sleeping 3s"
  sleep 3
done

echo "CockroachDB is up. Creating database 'market'..."
/cockroach/cockroach sql --insecure --host=cockroachdb-node1 -e "CREATE DATABASE IF NOT EXISTS market;"

echo "Applying schema to 'market'..."
cat /schema.sql | /cockroach/cockroach sql --insecure --host=cockroachdb-node1 -d market

echo "Schema applied successfully."
