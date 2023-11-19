#!/bin/bash
clickhouse-client --user clickhouse --password clickhouse --queries-file /docker-entrypoint-startdb.d/init_dwh_ch.sql