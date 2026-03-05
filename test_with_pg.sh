#!/bin/bash
set -e

PG_CONTAINER=blaster-test-pg
PG_PORT=5499
PG_USER=postgres
PG_PASS=postgres
PG_DB=postgres

# Clean up any existing container
docker stop $PG_CONTAINER 2>/dev/null || true
docker rm -f $PG_CONTAINER 2>/dev/null || true

# Start PostgreSQL
docker run --name $PG_CONTAINER \
	-e POSTGRES_USER=$PG_USER \
	-e POSTGRES_PASSWORD=$PG_PASS \
	-e POSTGRES_DB=$PG_DB \
	-p $PG_PORT:5432 \
	-d postgres:latest

# Wait until PostgreSQL is accepting connections
echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 30); do
	if docker exec $PG_CONTAINER pg_isready -U $PG_USER -q 2>/dev/null; then
		echo "PostgreSQL is ready."
		break
	fi
	if [ $i -eq 30 ]; then
		echo "PostgreSQL did not become ready in time."
		exit 1
	fi
	sleep 1
done

IS_TEST=1 /home/abhinav/any/python3_12_venv/bin/python3 -m unittest $1 $2
