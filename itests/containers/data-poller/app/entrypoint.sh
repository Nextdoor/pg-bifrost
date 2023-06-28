#!/bin/sh

# Please set this in your docker run config to specify which poller is used.
TRANSPORT_SINK=${TRANSPORT_SINK:-}

python3 /app/poller-$TRANSPORT_SINK.py
