#!/bin/bash

echo 'Tests - Enabling Replication'

# Allow remote replication connections
echo 'host replication all all md5' >> "$PGDATA/pg_hba.conf"


cat << EOF >> "$PGDATA/postgresql.conf"
log_connections = on
log_disconnections = on
log_statement = 'all'
log_replication_commands = on
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
