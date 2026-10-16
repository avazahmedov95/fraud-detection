#!/bin/bash
# The services' user, fraud_app: it adds rows and reads them, and cannot change or
# delete any, the audit trail's included. CLICKHOUSE_USER stays the administrator.
# On an existing database: docker exec clickhouse bash /docker-entrypoint-initdb.d/04-app-user.sh
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
CREATE USER OR REPLACE fraud_app IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_APP_PASSWORD}';
GRANT SELECT, INSERT ON fraud.* TO fraud_app;
SQL
