# Sink-writer: transactions.scored -> ClickHouse, with a case for each hold.
# Build context is the repo root.
FROM python:3.12-slim

# lightgbm, which explains each hold, needs the OpenMP runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY sink-writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY sink-writer/ .
COPY case-manager/case.py case-manager/explain.py data-generator/integrity.py ./

CMD ["python", "-u", "consumer.py"]
