#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

# A fresh container/port per invocation avoids changing existing databases.
PG_CONTAINER="blaster-test-pg-orm-$$"
PG_IMAGE="${PG_TEST_IMAGE:-pgvector/pgvector:pg16}"
cleanup() { docker rm -fv "$PG_CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run --name "$PG_CONTAINER" --rm \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres \
    -p 127.0.0.1::5432 -d "$PG_IMAGE" >/dev/null

for i in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -U postgres -q 2>/dev/null; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        docker logs "$PG_CONTAINER"
        exit 1
    fi
    sleep 1
done

export PG_TEST_HOST=127.0.0.1
export PG_TEST_PORT="$(docker port "$PG_CONTAINER" 5432/tcp | head -1 | cut -d: -f2)"
export PG_TEST_USER=postgres PG_TEST_PASSWORD=postgres PG_TEST_DB=postgres IS_TEST=1
PYTHON_BIN="${PG_TEST_PYTHON:-/home/abhinav/any/python3_14_venv/bin/python3}"
if [ "$#" -gt 0 ]; then
    "$PYTHON_BIN" -m unittest "$@"
else
    "$PYTHON_BIN" -m unittest tests.test_pg_sync_core
    # Runtime patches are consumer-owned and tested in separate processes.
    "$PYTHON_BIN" -m unittest tests.pg_test tests.test_pg_reload
    "$PYTHON_BIN" -m unittest tests.pg_gevent_test
    "$PYTHON_BIN" -m unittest tests.pg_async_test
fi
