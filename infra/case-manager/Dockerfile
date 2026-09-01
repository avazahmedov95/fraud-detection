# Case-manager: consumes fraud.alerts and materialises the analyst work queue.
#
# Its own image rather than a second process in the sink-writer container: the
# two read different topics with different freshness requirements, and a queue
# a person watches should not go down because the warehouse writer was restarted.
FROM python:3.11-slim

# LightGBM's compiled library links against the OpenMP runtime, and
# python:3.11-slim does not ship it. Without this the wheel installs cleanly and
# then fails at import with
#
#   OSError: libgomp.so.1: cannot open shared object file
#
# which is not a Python-level error and does not show up in any dependency
# check - the service starts, consumes alerts, and files every one of them as
# NO_MODEL. ~150 KB, in its own layer so it caches independently of the wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY case-manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY case-manager/ /app/

# The table DDL travels with the service. ClickHouse runs the scripts in
# docker-entrypoint-initdb.d only when its data directory is EMPTY, so on any
# cluster that has been up before this file would never execute and every
# insert would fail against a table that does not exist. store.py applies it on
# every connect instead; CREATE TABLE IF NOT EXISTS makes that free.
COPY infra/clickhouse/init/02-cases.sql /app/02-cases.sql

# The model, for explanations. model.txt is the bare Booster written by
# ml/export_onnx.py from the same object it converts to ONNX, so the trees that
# explain a decision are the trees that made it. feature_names.json labels the
# contributions: the booster's own names are Column_0..23, and labelling a
# contribution with the wrong feature would be the worst outcome available here
# — a confident, specific, wrong reason. explain.py checks the count and
# recomputes the probability before it will say anything.
#
# Absent artefacts are tolerated: the queue still opens cases, marked
# NO_MODEL. Run `python ml/export_onnx.py` if this build has no model.txt.
COPY ml/models/model.tx[t] ml/models/feature_names.jso[n] /app/

CMD ["python", "consumer.py"]
