# The training environment, pinned by ml/requirements.txt, in a container: on the
# owner's Windows, Smart App Control blocks the native library onnx loads
# (ml_dtypes). The repository is mounted at /repo, so files land in ml/models.
# Two uses: run.ps1 export-model runs the default command (ml/export_onnx.py:
# model.onnx, model.txt, manifest.json), and the retrainer service runs
# ml/retrain.py every day (docker-compose.yml).
FROM python:3.11-slim

# lightgbm links against the OpenMP runtime, which python:3.11-slim does not ship.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY ml/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /repo/ml
CMD ["python", "export_onnx.py"]
