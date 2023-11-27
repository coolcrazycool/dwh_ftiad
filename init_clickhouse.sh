#!/bin/bash
clickhouse-client --user default --queries-file /docker-entrypoint-startdb.d/init_dwh_ch.sql