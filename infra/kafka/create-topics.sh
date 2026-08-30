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
# transactions after enrichment + CEP + ML scoring
create transactions.scored  6
# high-risk decisions for downstream consumers
create fraud.alerts         3
# outbound reports for the Central Bank platform integration
create cbu.reports          1

echo "--- topics ---"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
echo "topics ready"
