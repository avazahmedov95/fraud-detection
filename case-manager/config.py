"""Configuration for the case-manager service.

Consumes fraud.alerts and materialises the analyst work queue in ClickHouse.
Defaults assume the Docker network service names.
"""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "fraud.alerts")

# A group of its own, deliberately. The sink-writer reads transactions.scored
# for the warehouse; this reads fraud.alerts for the queue. Sharing a group id
# across two topics and two services would let one service's offset commits
# stand in for the other's work.
CONSUMER_GROUP = os.getenv("CASE_CONSUMER_GROUP", "fraud-case-manager")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_ch")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")

BATCH_SIZE = int(os.getenv("CASE_BATCH_SIZE", "200"))
# Shorter than the sink-writer's 5 s. The queue is something a person watches,
# so freshness is the point; the warehouse path has no such requirement.
FLUSH_INTERVAL_S = float(os.getenv("CASE_FLUSH_INTERVAL_S", "2"))
