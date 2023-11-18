#!/bin/bash
set -e

echo "wal_level = logical" >> /var/lib/postgresql/data/postgresql.conf

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER $POSTGRES_REPLICA_USER WITH REPLICATION ENCRYPTED PASSWORD '$POSTGRES_REPLICA_PASSWORD';
    SELECT * FROM pg_create_physical_replication_slot('replication_slot_slave1');
EOSQL

pg_basebackup -U $POSTGRES_REPLICA_USER -D /var/lib/postgresql/slave -S replication_slot_slave1 -P -Fp -Xs -R

echo "primary_conninfo='host=$POSTGRES_HOST port=$POSTGRES_PORT user=$POSTGRES_REPLICA_USER password=$POSTGRES_REPLICA_PASSWORD'" >> /var/lib/postgresql/slave/postgresql.auto.conf
echo "primary_slot_name = 'replication_slot_slave1'" >> /var/lib/postgresql/slave/postgresql.auto.conf
echo "host    replication     $POSTGRES_REPLICA_USER      0.0.0.0/0               trust" >> /var/lib/postgresql/data/pg_hba.conf