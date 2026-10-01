"""Configuration for the second-look service: reads fraud.second_look, publishes the
final decision. Defaults are the Docker network names and the container's mounts."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SECOND_LOOK = os.getenv("TOPIC_SECOND_LOOK", "fraud.second_look")
TOPIC_SCORED = os.getenv("TOPIC_SCORED", "transactions.scored")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "fraud.alerts")

# A group of its own, as every consumer here has: shared, one service's commits
# would stand in for the other's work.
CONSUMER_GROUP = "fraud-second-look"

# second_look.json and .npz, written by ml/second_look.py; mounted read-only.
MODELS_DIR = os.getenv("MODELS_DIR", "/models")
# TabPFN's weights are a gated download, fetched by hand: TABPFN_CHECKPOINT in .env.
CHECKPOINT = os.getenv("TABPFN_CHECKPOINT", "/tabpfn/checkpoint.safetensors")
