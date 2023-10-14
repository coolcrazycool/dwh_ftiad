#!/bin/bash
set -e

cp -f /etc/postgresql/postgresql.conf /var/lib/postgresql/data
echo "host    replication     $POSTGRES_REPLICA_USER      0.0.0.0/0               trust" >> /var/lib/postgresql/data/pg_hba.conf