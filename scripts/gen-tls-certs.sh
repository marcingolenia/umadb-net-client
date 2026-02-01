#!/usr/bin/env bash
# Generates a CA and a server certificate for localhost TLS (tests / local UmaDB).
# Output: Tests/certs/ca.pem, server.pem, server-key.pem (and ca-key.pem).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/../Tests/certs"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

# CA key and cert (no passphrase for scripts/CI)
openssl genrsa -out ca-key.pem 2048
openssl req -x509 -new -nodes -key ca-key.pem -sha256 -days 1825 -out ca.pem \
  -subj "/CN=UmaDB Test CA"

# Server key and CSR with SAN for localhost
openssl req -new -newkey rsa:2048 -nodes \
  -keyout server-key.pem -out server.csr \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# Sign server cert with CA (preserve SAN)
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server.pem -days 365 -sha256 -copy_extensions copy

rm -f server.csr
echo "Generated in $OUT_DIR:"
ls -la ca.pem server.pem server-key.pem ca-key.pem 2>/dev/null || true
