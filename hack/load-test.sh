#!/usr/bin/env bash
# Postgres load tests: pgbench's built-in workloads through libpq (pgbench) and through may_postgres
# (examples/pgload.rs), against the same database, then one report comparing the two.
#
#   hack/load-test.sh                       # PGHOST/PGPORT/PGUSER/PGDATABASE pick the server
#
# Environment (defaults in brackets):
#   PGHOST [127.0.0.1] PGPORT [5433] PGUSER [postgres] PGDATABASE [postgres]
#   LOAD_SCALE [10]      pgbench -i scale (100,000 accounts per unit)
#   LOAD_CLIENTS [8]     connections per scenario
#   LOAD_DURATION [20]   measured seconds per run; LOAD_WARMUP [5] unmeasured seconds before
#   LOAD_MIN_RATIO [0.5] fail when may_postgres reaches less than this share of pgbench's tps
#   LOAD_OUT [target/load]
#
# Every scenario runs; the report then fails when any transaction failed, a run produced none or
# its output is missing, or may_postgres is below LOAD_MIN_RATIO. Absolute
# numbers on shared CI runners are noisy; the ratio between two tools measured back to back on the
# same runner and server is the stable signal.
set -euo pipefail
cd "$(dirname "$0")/.."

export PGHOST="${PGHOST:-127.0.0.1}" PGPORT="${PGPORT:-5433}" PGUSER="${PGUSER:-postgres}" PGDATABASE="${PGDATABASE:-postgres}"
SCALE="${LOAD_SCALE:-10}" CLIENTS="${LOAD_CLIENTS:-8}" DURATION="${LOAD_DURATION:-20}" WARMUP="${LOAD_WARMUP:-5}"
OUT="${LOAD_OUT:-target/load}"
PGBENCH="$(command -v pgbench || ls /usr/lib/postgresql/*/bin/pgbench 2>/dev/null | sort -V | tail -1 || true)"
[ -x "$PGBENCH" ] || { echo "load-test: pgbench not found" >&2; exit 2; }
JOBS=$(( CLIENTS < $(nproc) ? CLIENTS : $(nproc) ))
URL="host=$PGHOST port=$PGPORT user=$PGUSER dbname=$PGDATABASE"

rm -rf "$OUT" && mkdir -p "$OUT"
echo "== pgbench -i -s $SCALE ($("$PGBENCH" --version))"
"$PGBENCH" -i -q -s "$SCALE" >/dev/null
cargo build --release --example pgload --quiet
PGLOAD=target/release/examples/pgload

# pgbench: warm up, then a measured run with per-transaction latencies logged (10% sample).
pgbench_run() { # name, pgbench workload flags...
  local name=$1; shift
  "$PGBENCH" -M prepared -c "$CLIENTS" -j "$JOBS" -T "$WARMUP" "$@" >/dev/null 2>&1
  "$PGBENCH" -M prepared -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -l --sampling-rate=0.1 \
    --log-prefix="$OUT/pgbench-$name.log" "$@" > "$OUT/pgbench-$name.out" 2>&1 \
    || echo "  pgbench $name exited non-zero (see $OUT/pgbench-$name.out)"
  grep -E "^tps|latency average|failed" "$OUT/pgbench-$name.out" | sed "s/^/  pgbench $name: /"
}

pgload_run() { # name, pgload flags...
  local name=$1; shift
  "$PGLOAD" --url "$URL" --scale "$SCALE" --duration "$DURATION" --warmup "$WARMUP" \
    --json "$OUT/pgload-$name.json" "$@" 2>&1 | sed "s/^/  /" \
    || echo "  pgload $name exited non-zero"
}

echo "== select-only ($CLIENTS connections)"
pgbench_run select-only -S
pgload_run select-only --workload select-only --connections "$CLIENTS"

echo "== tpcb-like ($CLIENTS connections)"
pgbench_run tpcb-like
pgload_run tpcb-like --workload tpcb-like --connections "$CLIENTS"

echo "== simple-update ($CLIENTS connections)"
pgbench_run simple-update -N
pgload_run simple-update --workload simple-update --connections "$CLIENTS"

# may_postgres only: 16 coroutines pipelining on each of 2 connections. pgbench has no equivalent.
echo "== select-only, pipelined (2 connections x 16 coroutines)"
pgload_run select-only-pipelined --workload select-only --connections 2 --per-connection 16

status=0
python3 hack/load-report.py --dir "$OUT" --min-ratio "${LOAD_MIN_RATIO:-0.5}" \
  --output-json "$OUT/load-report.json" > "$OUT/load-report.md" || status=$?
cat "$OUT/load-report.md"
exit "$status"
