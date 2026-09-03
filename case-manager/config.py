"""Configuration for the case-manager service: consumes fraud.alerts and
materialises the analyst work queue in ClickHouse. Defaults are the Docker
network service names."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "fraud.alerts")

# A group of its own: sharing a group id across two topics and two services
# would let one service's offset commits stand in for the other's work.
CONSUMER_GROUP = "fraud-case-manager"

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_ch")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")

BATCH_SIZE = 200
FLUSH_INTERVAL_S = 2.0  # shorter than the sink-writer's 5 s; a person watches this queue
