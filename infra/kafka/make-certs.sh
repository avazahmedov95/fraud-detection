#!/bin/sh
# A private CA, a broker certificate and a client certificate for mutual TLS on
# Kafka; run by `run.ps1 make-certs`. Existing certificates stay unless FORCE=1.
# Development only: the CA's private key sits beside the certificates it signs.
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

# Reached as `kafka` inside the compose network and as `localhost` from the host:
# both names must be in the SAN, or hostname verification fails on one.
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

# The client takes PEM: kafka-python and Flink's Java client both read it.
echo "==> PEM keystore (client)"
cat client.key client.crt > client.keystore.pem

# The broker cannot: apache/kafka's entrypoint requires password files for its
# keystore, and an unencrypted PEM key has no password. PKCS12 it is.
echo "==> PKCS12 keystore (broker)"
STORE_PASS="${STORE_PASS:-fraudkafka}"
openssl pkcs12 -export -in kafka.crt -inkey kafka.key -certfile ca.crt \
  -name kafka -out kafka.keystore.p12 -passout "pass:${STORE_PASS}"

# The entrypoint reads these files, not environment variables.
printf '%s' "$STORE_PASS" > keystore_creds
printf '%s' "$STORE_PASS" > key_creds
printf '%s' "$STORE_PASS" > truststore_creds

rm -f kafka.csr client.csr kafka.ext client.ext

# Readable by uid 1000, the Kafka image's user: openssl writes the keystore 0600,
# and the broker then fails with a message that names no permission problem. The
# CA's and the broker's keys are read by no one after this script.
chmod 644 ./* 2>/dev/null || true
chmod 600 ca.key kafka.key

echo
echo "--- generated in $CERTS ---"
ls -1
echo
openssl verify -CAfile ca.crt kafka.crt
openssl verify -CAfile ca.crt client.crt
