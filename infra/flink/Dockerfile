# Flink with Python and PyFlink: the job runs in Python and serves the ONNX model.

ARG FLINK_VERSION=1.19.1
FROM flink:${FLINK_VERSION}-java11

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Pinned exactly, so a rebuild does not change the code the figures were measured on.
# apache-flink pins the rest (apache-beam 2.48.0, numpy 1.24.4, pandas 2.3.3,
# protobuf 4.23.4, py4j 0.10.9.7, cloudpickle 2.2.1).
ARG FLINK_VERSION
RUN pip3 install --no-cache-dir \
    apache-flink==${FLINK_VERSION} \
    onnxruntime==1.23.2 \
    redis==8.0.1 \
    cryptography==48.0.0

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# Owned by flink: a volume mounted over a missing path is root's, and the job then
# cannot write its checkpoints.
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
