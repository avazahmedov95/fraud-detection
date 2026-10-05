# The demo page (demo/), in the stack: what it sends is stamped on the containers'
# clock, the one the stage times use. The code is mounted, not copied.
FROM python:3.11-slim

# lightgbm, for the explainer, needs the OpenMP runtime the slim image lacks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY demo/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /repo
CMD ["python", "-u", "demo/server.py"]
