#!/usr/bin/env bash
# Starts the PostgreSQL servers the stream store tests run against, 14 and 17, in Docker on free
# ports, with TLS on from the image's self-signed certificate. Prints the variables to export.
# `stop` stops them. STREAMS_POSTGRES_NAME moves the container names, so two runs can share a
# host.
#
#   eval "$(crates/streams/scripts/postgres-test-server.sh)"
#   STREAMS_POSTGRES_URL=postgres://postgres:streams@localhost:<port>/postgres       (17)
#   STREAMS_POSTGRES_14_URL=postgres://postgres:streams@localhost:<port>/postgres    (14)
#   STREAMS_POSTGRES_TLS_URL=$STREAMS_POSTGRES_URL
set -euo pipefail

NAME=${STREAMS_POSTGRES_NAME:-streams-postgres}
VERSIONS=(14 17)

if [ "${1:-}" = stop ]; then
  for version in "${VERSIONS[@]}"; do
    docker rm -f "$NAME-$version" >/dev/null 2>&1 || true
  done
  exit 0
fi

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("", 0)); print(s.getsockname()[1])'
}

for version in "${VERSIONS[@]}"; do
  container="$NAME-$version"
  if ! docker inspect "$container" >/dev/null 2>&1; then
    docker run -d --rm --name "$container" -e POSTGRES_PASSWORD=streams \
      -p "127.0.0.1:$(free_port):5432" "postgres:$version" \
      -c ssl=on \
      -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
      -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key \
      -c max_connections=400 >/dev/null
  fi
  port=$(docker port "$container" 5432/tcp | head -1 | sed 's/.*://')
  for _ in $(seq 100); do
    docker exec "$container" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1 && break
    sleep 0.2
  done
  eval "URL_$version=postgres://postgres:streams@localhost:$port/postgres"
done

echo "export STREAMS_POSTGRES_URL=$URL_17"
echo "export STREAMS_POSTGRES_14_URL=$URL_14"
echo "export STREAMS_POSTGRES_TLS_URL=$URL_17"
