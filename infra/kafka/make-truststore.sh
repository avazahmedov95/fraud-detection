#!/bin/sh
# Second stage of certificate generation: the broker's truststore.
#
# Runs in the Kafka image rather than the openssl one, because it needs keytool
# and the Kafka image already ships a JDK - so the store is written by exactly
# the runtime that will read it.
#
# Why not openssl for this too, as for the keystore: a PKCS12 built with
# `openssl pkcs12 -export -nokeys` is readable by openssl but Java sees ZERO
# entries in it. Certificate-only bags written that way do not become
# trustedCertEntry, so the broker would start with an empty truststore and
# reject every client certificate - a failure that looks like a client problem.
# Verified rather than assumed:
#
#     openssl-built truststore -> "Your keystore contains 0 entries"
#     keytool-built truststore -> "fraud-ca, trustedCertEntry"
set -e

CERTS="${CERTS:-/certs}"
STORE_PASS="${STORE_PASS:-fraudkafka}"

cd "$CERTS"

if [ ! -f ca.crt ]; then
  echo "ca.crt not found in $CERTS - run make-certs.sh first"
  exit 1
fi

# This stage runs as the image's own user (uid 1000), which is also the user the
# broker runs as. If the material written by the previous stage is not readable
# here, it will not be readable by the broker either - so check it now, where
# the message can say so, rather than later as an SSL initialisation failure.
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
