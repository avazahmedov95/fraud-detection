# Build and run IBM AMLSim without touching the upstream source.
#
# WHY A CONTAINER. AMLSim's requirements.txt pins networkx==1.11 (2016), and
# scripts/transaction_graph_generator.py calls
#
#     nx.set_edge_attributes(g, 'active', False)
#
# in two places - the networkx 1.x argument order (G, name, values), reversed in
# 2.x to (G, values, name). On a modern networkx that is wrong rather than loud.
# The alternative to this file is patching two lines of somebody else's
# generator, which would forfeit the reason AMLSim was chosen over a commercial
# dataset: that a reader can reproduce the data by running the published code
# unmodified. Pinning the interpreter instead keeps that claim.
#
# Also note: pygraphviz and matplotlib are listed in requirements.txt but are
# imported by none of the scripts this run uses. They are the painful
# dependencies on Windows, and they are skipped deliberately - see the pip line.
#
#   docker build -f amlsim.Dockerfile -t amlsim:1.0 .
#   docker run --rm -v "C:/path/to/AMLSim:/amlsim" -w /amlsim amlsim:1.0 \
#       bash -lc "bash scripts/build_AMLSim.sh && \
#                 python scripts/transaction_graph_generator.py conf.json && \
#                 bash scripts/run_AMLSim.sh conf.json"
#
# Outputs land in the mounted AMLSim/outputs/<simulation_name>/, which is then
# the --dir argument to amlsim_adapter.py.

FROM python:3.8-slim

# default-jdk on this base is JDK 17, which still accepts AMLSim's
# maven.compiler.source=1.8 with a deprecation warning.
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jdk maven bash \
    && rm -rf /var/lib/apt/lists/*

# Exactly what the generator imports, and nothing else. Installing the full
# requirements.txt drags in pygraphviz, which needs Graphviz dev headers and a
# compiler, for a library no script here imports.
RUN pip install --no-cache-dir \
        "numpy<1.25" "networkx==1.11" powerlaw python-dateutil Faker

# MASON is not on Maven Central. It is George Mason University's simulation
# toolkit, AMLSim's pom declares it as mason:mason:jar:20, and the build fails
# with "Could not find artifact mason:mason:jar:20 in central" until it is
# installed by hand. AMLSim's own README documents the step - fetch the one jar,
# then `mvn install:install-file` - but its URL is stale in two ways, and both
# were found the expensive way:
#
#   1. The site moved to people.cs.gmu.edu; cs.gmu.edu now redirects to a page.
#   2. From version 18 MASON ships as a BARE JAR, not a zip. There is no
#      mason20.zip. Requesting one returns a 54 KB HTML page, and `curl -fSL`
#      reports success because the server answered 200 - the download "worked"
#      and produced the wrong file. Only unzip failed, one step later, pointing
#      at the archive rather than at the URL.
#
# Hence `unzip -tq` below on a file that needs no unpacking: a jar is a zip, so
# testing its central directory is a cheap integrity check that fails HERE, on
# the download, instead of somewhere downstream. The same class of defect this
# project catalogues elsewhere - the step that reports success while producing
# something unusable.
#
# Downloads mason.20.jar (a few MB). If the URL moves again the build stops on
# this layer: drop the jar into AMLSim/jars/ yourself and run the
# install:install-file line against it.
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/mason \
    && curl -fSL -o /opt/mason/mason.20.jar \
         https://people.cs.gmu.edu/~eclab/projects/mason/mason.20.jar \
    && unzip -tq /opt/mason/mason.20.jar > /dev/null \
    && mvn -q install:install-file \
         -Dfile=/opt/mason/mason.20.jar \
         -DgroupId=mason -DartifactId=mason -Dversion=20 \
         -Dpackaging=jar -DgeneratePom=true \
    && test -s /root/.m2/repository/mason/mason/20/mason-20.jar

# The jar is KEPT at /opt/mason/, outside ~/.m2, on purpose. Mounting a named
# volume at /root/.m2 to cache Maven downloads shadows whatever the image put
# there - and Docker only seeds a named volume from the image on its FIRST
# mount, so an existing volume silently hides MASON and the build fails with the
# same "not found in central" error it was meant to fix. This entrypoint
# reinstalls from /opt/mason on every start, which is a no-op when it is already
# present and a repair when a volume has hidden it. Cheap, and it removes a
# failure mode whose symptom points at the wrong cause.
RUN printf '%s\n' '#!/usr/bin/env bash' 'set -e' \
  'if [ ! -s /root/.m2/repository/mason/mason/20/mason-20.jar ]; then' \
  '  echo "[amlsim] MASON missing from ~/.m2 (shadowed by a volume?) - reinstalling"' \
  '  mvn -q install:install-file -Dfile=/opt/mason/mason.20.jar \' \
  '    -DgroupId=mason -DartifactId=mason -Dversion=20 \' \
  '    -Dpackaging=jar -DgeneratePom=true' \
  'fi' 'exec "$@"' > /usr/local/bin/entrypoint.sh \
  && chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /amlsim
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash"]
