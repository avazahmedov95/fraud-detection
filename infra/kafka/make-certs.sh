#!/bin/sh
# Generate a private CA, a broker certificate and a client certificate for
# mutual TLS on Kafka (reviewer point 3, transport half).
#
# PEM, not JKS. Kafka 3.x and the Java client both accept ssl.keystore.type=PEM,
# so one set of files serves the broker, the Java client inside Flink and the
# Python clients (kafka-python takes ssl_cafile/certfile/keyfile directly).
# Going through keytool would mean maintaining two formats and two passwords for
# no gain.
#
# Idempotent: existing certificates are left alone unless FORCE=1, because
# regenerating them mid-experiment would invalidate the arm being measured.
#
#   docker run --rm -v "$PWD/infra/kafka/certs:/certs" -e DAYS=3650 \
#     alpine/openssl:latest sh /certs/../make-certs.sh
#
# DEVELOPMENT ONLY. The CA private key sits next to the certificates it signs,
# which is exactly what a real deployment must not do; it exists so the
# measurement can be reproduced from this repository alone.
set -e

CERTS="${CERTS:-/certs}"
DAYS="${DAYS:-3650}"
SUBJ_BASE="/C=UZ/ST=Tashkent/L=Tashkent/O=fraud-detection-prototype"

mkdir -p "$CERTS"
cd "$CERTS"

if [ -f ca.crt ] && [ "$FORCE" != "1" ]; then
  echo "certificates already present in $CERTS (FORCE=1 to regenerate)"
  exit 0
fi

echo "==> private CA"
openssl req -x509 -newkey rsa:2048 -days "$DAYS" -nodes \
  -keyout ca.key -out ca.crt \
  -subj "${SUBJ_BASE}/CN=fraud-detection-ca" 2>/dev/null

# The broker is reached as `kafka` from inside the compose network and as
# `localhost` from the host, so both names must be in the SAN or hostname
# verification fails on one of them. Leaving them out and disabling
# verification instead would make the measurement meaningless: the handshake
# cost being measured is partly the certificate checking.
echo "==> broker certificate (SAN: kafka, localhost)"
openssl req -newkey rsa:2048 -nodes -keyout kafka.key -out kafka.csr \
  -subj "${SUBJ_BASE}/CN=kafka" 2>/dev/null
cat > kafka.ext <<EOF
subjectAltName = DNS:kafka, DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth, clientAuth
EOF
openssl x509 -req -in kafka.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out kafka.crt -days "$DAYS" -extfile kafka.ext 2>/dev/null

echo "==> client certificate"
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
  -subj "${SUBJ_BASE}/CN=fraud-pipeline-client" 2>/dev/null
cat > client.ext <<EOF
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$DAYS" -extfile client.ext 2>/dev/null

# Clients take PEM: kafka-python reads ssl_cafile/certfile/keyfile directly, and
# the Java client in Flink has accepted ssl.keystore.type=PEM since Kafka 2.7.
echo "==> PEM keystores (clients)"
cat kafka.key kafka.crt > kafka.keystore.pem
cat client.key client.crt > client.keystore.pem

# The BROKER cannot use PEM here, and the reason is the image rather than Kafka.
# apache/kafka's entrypoint script, on seeing SSL:// in the advertised
# listeners, overrides ssl.keystore.location with
# /etc/kafka/secrets/$KAFKA_SSL_KEYSTORE_FILENAME and *requires* credential
# files for the keystore and key passwords. An unencrypted PEM key has no
# password to give it. PKCS12 is what the script is built for.
echo "==> PKCS12 keystore (broker)"
STORE_PASS="${STORE_PASS:-fraudkafka}"
openssl pkcs12 -export -in kafka.crt -inkey kafka.key -certfile ca.crt \
  -name kafka -out kafka.keystore.p12 -passout "pass:${STORE_PASS}"

# The entrypoint reads these files, not environment variables.
printf '%s' "$STORE_PASS" > keystore_creds
printf '%s' "$STORE_PASS" > key_creds
printf '%s' "$STORE_PASS" > truststore_creds

rm -f kafka.csr client.csr kafka.ext client.ext

# Everything here must be readable by uid 1000. This container runs as root,
# but the Kafka image runs as `appuser` (uid 1000) and so does the keytool step
# that builds the truststore - and openssl writes the PKCS12 keystore 0600.
# Missing this produced "Keystore file does not exist: kafka.keystore.p12" from
# keytool and a broker that shut down during SSL initialisation, neither of
# which names a permission problem.
#
# Safe only because this is a throwaway development CA whose private key is
# already sitting in the same directory. Real key material would be mounted
# from a secret store with restrictive ownership instead.
chmod 644 ./* 2>/dev/null || true

echo
echo "--- generated in $CERTS ---"
ls -1
echo
openssl verify -CAfile ca.crt kafka.crt
openssl verify -CAfile ca.crt client.crt
