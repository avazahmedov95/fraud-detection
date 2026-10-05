# The training environment (ml/requirements.txt) in a container, since Windows' Smart
# App Control blocks a library onnx loads. Used by run.ps1 export-model and by the
# retrainer service; the repository is mounted at /repo.
FROM python:3.11-slim

# lightgbm needs the OpenMP runtime the slim image lacks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY ml/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /repo/ml
CMD ["python", "export_onnx.py"]
