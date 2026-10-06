#!/usr/bin/env bash
# Run nativeclick integration tests against several ClickHouse server versions.
# Usage: docs/test-fork.sh [version...]   -> results in docs/results/<version>.log
set -u
cd "$(dirname "$0")/.."
# Default: the supported window (targeted version + the 10 releases before it).
VERSIONS=${*:-"25.11 25.12 26.1 26.2 26.3 26.4 26.5 26.6 26.7 26.8 26.9"}
PORT=19000
mkdir -p docs/results
export NATIVECLICK_TEST_ADDR=127.0.0.1:$PORT

for v in $VERSIONS; do
  echo "=== $v"
  docker rm -f kh-test >/dev/null 2>&1
  docker run -d --name kh-test -p $PORT:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    --ulimit nofile=262144:262144 clickhouse/clickhouse-server:$v >/dev/null || { echo "$v: pull/run failed"; continue; }
  for _ in $(seq 60); do docker exec kh-test clickhouse-client -q 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done
  # ponytail: global 300s timeout per version, per-test timeouts if a single hang hides others
  perl -e 'alarm 600; exec @ARGV' cargo test --all-features --test test -- --test-threads=1 \
    > docs/results/$v.log 2>&1
  echo "exit=$?" >> docs/results/$v.log
  grep -E '^test result|^exit' docs/results/$v.log
done
docker rm -f kh-test >/dev/null 2>&1
