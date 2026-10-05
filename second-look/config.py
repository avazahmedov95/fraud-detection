"""The second-look service's settings; defaults are the Docker network's and mounts'."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SECOND_LOOK = os.getenv("TOPIC_SECOND_LOOK", "fraud.second_look")
TOPIC_SCORED = os.getenv("TOPIC_SCORED", "transactions.scored")

CONSUMER_GROUP = "fraud-second-look"

# How long a transfer may wait for its answer; held unscored past it. ALIVE_KEY
# lapses after the same time, and the job then holds band transfers itself.
DEADLINE_S = float(os.getenv("SECOND_LOOK_DEADLINE_S", "5"))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
ALIVE_KEY = "second-look:alive"

# second_look.json and .npz, written by ml/second_look.py; mounted read-only.
MODELS_DIR = os.getenv("MODELS_DIR", "/models")
# TabPFN's weights are a gated download, fetched by hand: TABPFN_CHECKPOINT in .env.
CHECKPOINT = os.getenv("TABPFN_CHECKPOINT", "/tabpfn/checkpoint.safetensors")
