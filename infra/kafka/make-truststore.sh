#!/bin/sh
# The broker's truststore, by keytool in the Kafka image: Java reads a PKCS12
# truststore made by openssl as zero entries, and the broker would then reject
# every client.
set -e

CERTS="${CERTS:-/certs}"
STORE_PASS="${STORE_PASS:-fraudkafka}"

cd "$CERTS"

if [ ! -f ca.crt ]; then
  echo "ca.crt not found in $CERTS - run make-certs.sh first"
  exit 1
fi

# This runs as the broker's own user: what it cannot read, the broker cannot either.
for f in ca.crt kafka.keystore.p12 keystore_creds key_creds truststore_creds; do
  if [ ! -r "$f" ]; then
    echo "ERROR: $f is not readable as $(id -u):$(id -g)"
    ls -l "$f" 2>/dev/null || echo "  (file missing entirely)"
    echo "The broker runs as this same user and would fail the same way."
    exit 1
  fi
done

rm -f kafka.truststore.p12
keytool -importcert -noprompt -alias fraud-ca -file ca.crt \
  -keystore kafka.truststore.p12 -storetype PKCS12 -storepass "$STORE_PASS"

echo
echo "--- truststore ---"
keytool -list -keystore kafka.truststore.p12 -storetype PKCS12 \
  -storepass "$STORE_PASS" | head -6
echo
echo "--- keystore ---"
keytool -list -keystore kafka.keystore.p12 -storetype PKCS12 \
  -storepass "$STORE_PASS" | head -6
