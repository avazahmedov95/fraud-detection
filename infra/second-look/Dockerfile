# The second look: TabPFN on the transfers just under the review cut-off
# (second-look/README.md). An image of its own: torch and TabPFN are more than a
# gigabyte the other services have no use for.
FROM python:3.11-slim

# LightGBM, which TabPFN depends on, links against the OpenMP runtime the slim
# image lacks (infra/case-manager/Dockerfile has the failure this prevents).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# The CPU build of torch first, from its own index: the default wheel carries CUDA,
# gigabytes a CPU-only machine never uses.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.14.0
COPY second-look/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY second-look/*.py /app/

# The context and the cut-offs (ml/second_look.py) and TabPFN's weights are mounted,
# not copied: a re-export or a new checkpoint needs a restart, not a rebuild.
CMD ["python", "-u", "consumer.py"]
