#!/usr/bin/env bash
# TLS integration tests (tests/tls.rs) against two throwaway Postgres containers: one TLS-only with client
# certificates for `certuser`, one plaintext. Generates its own CAs. Needs docker and openssl.
#
#   tests/tls/run.sh            # PG_IMAGE=postgres:17 TLS_PG_PORT=55440 PLAIN_PG_PORT=55441 to override
set -euo pipefail

here="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE="${PG_IMAGE:-postgres:17}"
TLS_PORT="${TLS_PG_PORT:-55440}"
PLAIN_PORT="${PLAIN_PG_PORT:-55441}"
dir="$(mktemp -d)"
tls_name="may-pg-tls-$$" plain_name="may-pg-plain-$$"
cleanup() { docker rm -f "$tls_name" "$plain_name" >/dev/null 2>&1 || true; rm -rf "$dir"; }
trap cleanup EXIT

cd "$dir"
ec() { openssl ecparam -name prime256v1 -genkey -noout -out "$1" 2>/dev/null; }
ec ca.key
openssl req -x509 -new -key ca.key -subj /CN=test-ca -days 2 -out ca.crt 2>/dev/null
ec other-ca.key
openssl req -x509 -new -key other-ca.key -subj /CN=other-ca -days 2 -out other-ca.crt 2>/dev/null
sign() {  # $1 name $2 CN $3 CA prefix $4 extensions
  ec "$1.key"
  openssl req -new -key "$1.key" -subj "/CN=$2" -out "$1.csr" 2>/dev/null
  printf '%s\n' "$4" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA "$3.crt" -CAkey "$3.key" -CAcreateserial -days 2 -extfile "$1.ext" -out "$1.crt" 2>/dev/null
}
sign server localhost ca "subjectAltName=DNS:localhost"
sign client certuser ca "extendedKeyUsage=clientAuth"
sign other certuser other-ca "extendedKeyUsage=clientAuth"
cat > pg_hba.conf <<'EOF'
local   all  all                trust
hostssl all  certuser  all      scram-sha-256 clientcert=verify-full
hostssl all  all       all      scram-sha-256
host    all  all       all      reject
EOF
mkdir srv
cp ca.crt server.crt server.key pg_hba.conf srv/
chmod 644 srv/*

docker run -d --name "$tls_name" -p "127.0.0.1:${TLS_PORT}:5432" -e POSTGRES_PASSWORD=postgres -v "$dir/srv:/src:ro" \
  --entrypoint bash "$IMAGE" -c 'mkdir -p /certs; cp /src/* /certs/; chown -R postgres /certs; chmod 600 /certs/server.key;
    exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key \
      -c ssl_ca_file=/certs/ca.crt -c hba_file=/certs/pg_hba.conf -c listen_addresses=*' >/dev/null
docker run -d --name "$plain_name" -p "127.0.0.1:${PLAIN_PORT}:5432" -e POSTGRES_PASSWORD=postgres "$IMAGE" >/dev/null

ready() { for _ in $(seq 1 60); do docker exec "$1" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1 && return 0; sleep 1; done; docker logs "$1"; return 1; }
ready "$tls_name"; ready "$plain_name"
# The entrypoint restarts the server after init; wait for the final one.
sleep 2; ready "$tls_name"; ready "$plain_name"
docker exec "$tls_name" psql -U postgres -qc "CREATE ROLE pw LOGIN PASSWORD 'pw'; CREATE ROLE certuser LOGIN PASSWORD 'cu'; GRANT pg_read_all_stats TO pw;" >/dev/null

cd "$here"
TLS_PG_PORT="$TLS_PORT" PLAIN_PG_PORT="$PLAIN_PORT" TLS_TEST_DIR="$dir" cargo test --test tls -- --test-threads=1 "$@"
