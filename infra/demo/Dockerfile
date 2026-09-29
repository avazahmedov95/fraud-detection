# The demo page (demo/). A container of the stack rather than a host process: it
# exists only while the stack does, and the transfers it sends are stamped on the
# containers' clock, the one the per-stage times are measured against. The host's
# clock drifts from it by hundreds of milliseconds.
#
# The code is mounted, not copied (docker-compose.yml): the page reads the
# generator's dataset, case-manager's store and explainer, and the model's files
# where the rest of the project keeps them.
FROM python:3.11-slim

# lightgbm, for case-manager's explainer, links against the OpenMP runtime, which
# python:3.11-slim does not ship (see infra/case-manager/Dockerfile).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY demo/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /repo
CMD ["python", "-u", "demo/server.py"]
