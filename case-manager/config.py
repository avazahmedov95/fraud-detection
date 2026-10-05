"""The analyst CLI's warehouse connection. Defaults are the Docker network names."""

import os

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")
