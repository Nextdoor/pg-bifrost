#!/bin/bash

# Start postgres in the background
export POSTGRES_PASSWORD=pgbifrost
nohup docker-entrypoint.sh postgres &

# Wait for postgres to start initially
sleep 5

# Wait until postgres is ready
i=0

# Wait for postgres to be ready
until pg_isready
do
  echo "Waiting for postgres to start..."

  if [ $i -eq 5 ]; then
    echo "Timed out..."
    exit 1
  fi
  sleep 2
  i=$((i+1))
done

# Remove the init files since they already ran
rm -rf docker-entrypoint-initdb.d
