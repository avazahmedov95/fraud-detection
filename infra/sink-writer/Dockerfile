# Sink-writer service: consumes transactions.scored -> ClickHouse + Neo4j.
# Build context is the repo root (so it can COPY sink-writer/).
FROM python:3.12-slim

WORKDIR /app
COPY sink-writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sink-writer/ .
# Columns added after the table was created; ch_writer.py applies the file on every
# connect, since ClickHouse runs its init scripts only on an empty data directory.
COPY infra/clickhouse/init/03-stages.sql .

CMD ["python", "-u", "consumer.py"]
