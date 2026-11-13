#!/bin/sh
# Runs every benchmark in bench/RESULTS.md: one server, two replicas sharing
# Redis, and TLS. Each figure is the median of 3 runs.
#
# From the repository root:
#   docker network create chatbench
#   docker run -d --name chatbench-redis --network chatbench redis:7.4-alpine \
#       redis-server --appendonly yes --appendfsync everysec
#   docker run --rm --network chatbench -v "$PWD":/src -w /src python:3.12-slim \
#       sh bench/run_suite.sh redis://chatbench-redis:6379/0
#   docker rm -f chatbench-redis; docker network rm chatbench
set -eu
REDIS_URL=${1:?usage: run_suite.sh REDIS_URL}
export CHAT_ENABLE_RATE_LIMITING=false CHAT_LOG_LEVEL=WARNING \
       CHAT_MAX_CONNECTIONS=20000 CHAT_MAX_CONNECTIONS_PER_IP=20000
B="python bench/bench_server.py"
SECRETS=$(mktemp -d)
python scripts/init_secrets.py "$SECRETS" --hosts localhost,127.0.0.1 >/dev/null

start() {  # start NAME PORT METRICS_PORT [ENV=VALUE ...]
  name=$1 port=$2 mport=$3; shift 3
  env CHAT_PORT="$port" CHAT_METRICS_PORT="$mport" "$@" python main.py >"/tmp/$name.log" 2>&1 &
  echo $! >"/tmp/$name.pid"
  for _ in $(seq 50); do
    python -c "import socket; socket.create_connection(('127.0.0.1', $port), 1)" 2>/dev/null && return
    sleep 0.2
  done
  echo "$name did not start"; cat "/tmp/$name.log"; exit 1
}
stop() { kill "$(cat "/tmp/$1.pid")"; wait "$(cat "/tmp/$1.pid")" 2>/dev/null || true; }
section() { echo; echo "### $*"; }

section "single server (history in memory)"
start single 8800 9100
P=$(cat /tmp/single.pid)
section "single: 50 members, 5 senders";  $B fanout --port 8800 --clients 50 --senders 5 --messages 2000 --server-pid $P --repeat 3
section "single: 1:1 pipelined";          $B fanout --port 8800 --clients 2 --senders 1 --messages 20000 --window 64 --server-pid $P --repeat 3
section "single: 1:1 latency";            $B fanout --port 8800 --clients 2 --senders 1 --messages 2000 --window 1 --server-pid $P --repeat 3
stop single

section "two replicas sharing Redis"
start r1 8801 9101 CHAT_REDIS_URL="$REDIS_URL" CHAT_REDIS_NAMESPACE=bench$$
start r2 8802 9102 CHAT_REDIS_URL="$REDIS_URL" CHAT_REDIS_NAMESPACE=bench$$
T=127.0.0.1:8801,127.0.0.1:8802
P=$(cat /tmp/r1.pid)
section "cluster: 50 members over 2 replicas, 5 senders"; $B fanout --targets $T --clients 50 --senders 5 --messages 2000 --server-pid $P --repeat 3
section "cluster: 1:1 across replicas, pipelined";        $B fanout --targets $T --clients 2 --senders 1 --messages 20000 --window 64 --server-pid $P --repeat 3
section "cluster: 1:1 across replicas, latency";          $B fanout --targets $T --clients 2 --senders 1 --messages 2000 --window 1 --server-pid $P --repeat 3
stop r1; stop r2

section "TLS + token login (single server)"
start tls 8803 9103 CHAT_TLS_CERT="$SECRETS/server.pem" CHAT_TLS_KEY="$SECRETS/server.key" \
      CHAT_AUTH_MODE=token CHAT_JWT_SECRET_FILE="$SECRETS/jwt.secret"
P=$(cat /tmp/tls.pid)
TLSARGS="--tls --ca $SECRETS/ca.pem --server-name localhost --token-secret-file $SECRETS/jwt.secret"
section "tls: 50 members, 5 senders"; $B fanout --port 8803 $TLSARGS --clients 50 --senders 5 --messages 2000 --server-pid $P --repeat 3
section "tls: 1:1 pipelined";         $B fanout --port 8803 $TLSARGS --clients 2 --senders 1 --messages 20000 --window 64 --server-pid $P --repeat 3
section "tls: 1:1 latency";           $B fanout --port 8803 $TLSARGS --clients 2 --senders 1 --messages 2000 --window 1 --server-pid $P --repeat 3
section "tls: 2,000 connections (full handshakes)"; $B connections --port 8803 $TLSARGS --count 2000 --server-pid $P
stop tls
rm -rf "$SECRETS"
