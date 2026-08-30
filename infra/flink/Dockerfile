# Flink session-cluster image with Python + PyFlink, so the stream processor
# (phase 4) can run Python jobs and serve the ONNX model inside Flink.

ARG FLINK_VERSION=1.19.1
FROM flink:${FLINK_VERSION}-java11

# Python toolchain.
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink matching the cluster version, plus the runtime libs the job will need.
#
# Pinned exactly. Unpinned floors here mean the image changes under the code
# every time it is rebuilt: a rebuild on 2026-08-11 moved the sink-writer image
# across three major versions at once with no change to this repository, and an
# image that no longer matches the code the measurements were taken on makes
# those measurements unreproducible.
#
# apache-flink==1.19.1 fixes its own dependency tree; the versions it resolved
# to on the build these figures come from are, for the record:
#   apache-beam 2.48.0 | numpy 1.24.4 | pandas 2.3.3
#   protobuf 4.23.4    | py4j 0.10.9.7 | cloudpickle 2.2.1
ARG FLINK_VERSION
RUN pip3 install --no-cache-dir \
    apache-flink==${FLINK_VERSION} \
    onnxruntime==1.23.2 \
    redis==8.0.1 \
    neo4j==6.2.0 \
    cryptography==48.0.0

# Kafka connector JAR for the SQL/DataStream Kafka source/sink.
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# Checkpoint directory for the retained checkpoints (see stream-processor
# config.CHECKPOINT_DIR), created here rather than left to the volume mount.
#
# docker-compose mounts the named volume `flink_checkpoints` at this path. A
# named volume mounted over a path that does NOT exist in the image is created
# by the daemon owned by root:root, and the Flink process runs as `flink` (the
# base image starts as root and drops privileges via gosu). The job then dies at
# submission with:
#
#   IOException: Failed to create directory for shared state:
#   file:/opt/flink/checkpoints/<job-id>/shared
#
# Creating the directory in the image with the right ownership fixes it: Docker
# populates an empty named volume from the image path, ownership included.
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
