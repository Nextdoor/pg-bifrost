#!/bin/bash

# Wait for postgres to be ready
i=0
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

# Do command
exec "$@"
