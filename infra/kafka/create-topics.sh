#!/bin/bash
# Create the pipeline topics. Idempotent — safe to re-run.
set -e

BOOTSTRAP="kafka:9092"
KT=/opt/kafka/bin/kafka-topics.sh

create () {
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$1" --partitions "$2" --replication-factor 1
}

# raw events from the payment switch (keyed by sender_card -> ordered per sender)
create transactions.raw     6
# every transaction after CEP + ML scoring
create transactions.scored  6
# high-risk decisions for downstream consumers
# transfers just under the review cut-off, waiting for TabPFN (second-look/README.md)
create fraud.second_look    3

# Kafka stamps each raw event with its own append time. The job takes event_time
# from the payload and reads this stamp only to split the wait before it into the
# hop into Kafka and the hop out (stream-processor/fraud_job.py STAGES).
/opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BOOTSTRAP" --alter \
    --entity-type topics --entity-name transactions.raw \
    --add-config message.timestamp.type=LogAppendTime

echo "--- topics ---"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
echo "topics ready"
