"""Configuration for the sink-writer service. Defaults are the Docker network names."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SCORED = os.getenv("TOPIC_SCORED", "transactions.scored")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "fraud-sink-writer")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")

BATCH_SIZE = int(os.getenv("SINK_BATCH_SIZE", "500"))
# A hold reaches the analyst's queue within this; MergeTree dislikes smaller inserts.
FLUSH_INTERVAL_S = float(os.getenv("SINK_FLUSH_INTERVAL_S", "2"))

# Audit every decision, or only the holds.
AUDIT_ALL = os.getenv("SINK_AUDIT_ALL", "true").lower() in ("1", "true", "yes")
