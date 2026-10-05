# The second look: TabPFN on the transfers just under the review cut-off. An image
# of its own: torch and TabPFN are over a gigabyte nothing else needs.
FROM python:3.11-slim

# LightGBM, which TabPFN depends on, needs the OpenMP runtime the slim image lacks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# The CPU build of torch: the default wheel carries gigabytes of CUDA.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.14.0
COPY second-look/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY second-look/*.py /app/

# The context, the cut-offs and the weights are mounted: a new one needs a restart.
CMD ["python", "-u", "consumer.py"]
