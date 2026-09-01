"""
Configuration for the sink-writer service (consumes transactions.scored and
persists to ClickHouse + Neo4j). Defaults assume the Docker network service names.
"""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SCORED = os.getenv("TOPIC_SCORED", "transactions.scored")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "fraud-sink-writer")

# ClickHouse (HTTP interface via clickhouse-connect).
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "fraud")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "fraud_ch")
CH_DB = os.getenv("CLICKHOUSE_DB", "fraud")

# Neo4j (bolt).
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "fraud_neo4j")

# Batching.
BATCH_SIZE = int(os.getenv("SINK_BATCH_SIZE", "500"))
# The warehouse path has no real-time requirement — by the time this runs the
# decision already EXISTS and has been published to fraud.alerts, and ClickHouse
# is where it is queried afterwards. (This used to say the decision "has already
# reached the switch", which named an integration that does not exist: nothing
# enforces a decision here. The case-manager consumes that topic and opens an
# analyst case; enforcement is out of scope and declared as such.) Batching here
# is therefore free latency-wise
# and good for MergeTree, which dislikes many small inserts. Lower it only if
# dashboards need to be fresher, not to chase the detection target.
FLUSH_INTERVAL_S = float(os.getenv("SINK_FLUSH_INTERVAL_S", "5"))

# Audit every decision (compliance-complete) vs only flagged (REVIEW/BLOCK).
AUDIT_ALL = os.getenv("SINK_AUDIT_ALL", "true").lower() in ("1", "true", "yes")
