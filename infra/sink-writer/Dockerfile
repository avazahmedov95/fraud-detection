# Sink-writer service: consumes transactions.scored -> ClickHouse + Neo4j.
# Build context is the repo root (so it can COPY sink-writer/).
FROM python:3.12-slim

WORKDIR /app
COPY sink-writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sink-writer/ .

CMD ["python", "-u", "consumer.py"]
