# Exports the trained model for the job (ml/export_onnx.py: model.onnx, model.txt,
# manifest.json) with the versions ml/requirements.txt pins. A container rather
# than the host's Python: on the owner's Windows, Smart App Control blocks the
# native library onnx loads (ml_dtypes). The repository is mounted at /repo
# (run.ps1 export-model), so the files land in ml/models as before.
FROM python:3.11-slim

# lightgbm links against the OpenMP runtime, which python:3.11-slim does not ship.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY ml/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /repo/ml
CMD ["python", "export_onnx.py"]
