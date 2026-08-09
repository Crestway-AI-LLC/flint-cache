#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The compatibility corpus, run THROUGH THE PROXY as a tenant.
#
# WHY THIS EXISTS. `tools/gates.sh` has always run the 99-case corpus against
# a bare flint-server: `--target 127.0.0.1:<node>`, no proxy, no tenant, no
# auth. That is not what anybody connects to. Every real client reaches Flint
# through the proxy, and the proxy is not a pipe — it terminates the client's
# RESP dialect, speaks its own to the backend, pins transactions, routes by
# slot and re-encodes every reply. None of that was ever measured by the
# corpus.
#
# The first run of this drill found a bug that had been shipping the whole
# time: an aborted EXEC returned `$-1` (null BULK) to RESP2 clients instead of
# `*-1` (null ARRAY). The node was right and the gate was green, because the
# corpus never met the component that broke it. The proxy always sends
# `HELLO 3` to backends, RESP3 has exactly ONE null, and the array-ness was
# destroyed on the backend hop and never rebuilt.
#
# So the general rule this file encodes: a check that skips the edge cannot
# see edge bugs, and "the server is conformant" is not the claim customers
# rely on.
#
# BOTH DIALECTS, and RESP3 is not the afterthought. redis-py 8 and node-redis
# negotiate it by default, so it is what most clients actually speak; it was
# also the last thing here with no gate at all. It is a different protocol,
# not a re-spelling — one null instead of two — and that difference is
# exactly what hid the EXEC bug above, so running only RESP2 through the
# proxy would leave the more common path unmeasured.
#
# Requires: a release build with --features rocks.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-proxyconf-state 7960 7961 7962 7963
fleet_guard
D=/tmp/flint-proxyconf; STATE=/tmp/flint-proxyconf-state
INV=$D/cluster.flint
rm -rf "$D" "$STATE"; mkdir -p "$D"

fleet_kill server; fleet_kill proxy
fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  ./target/release/flintctl -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy
  fleet_kill controlplane; fleet_kill controller
  [ -n "${KEEP:-}" ] || rm -rf "$D" "$STATE"
}
trap cleanup EXIT

# Guarded: unguarded, a failed build leaves the drill asserting against
# whatever binaries were already in target/release — a drill certifying a
# change it never compiled.
cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl -p flint-conformance --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $STATE
bins ./target/release
cp 127.0.0.1:7963
pair 127.0.0.1:7960,127.0.0.1:7961
proxy 127.0.0.1:7962
controller on
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap"
$CTL bootstrap >"$D/boot.log" 2>&1 || { echo "FAIL: bootstrap"; tail -8 "$D/boot.log"; exit 1; }
$CTL tenant add conf tok-conf conf 1 >/dev/null 2>&1 || { echo "FAIL: tenant add"; exit 1; }
for _ in $(seq 1 60); do
  [ "$(valkey-cli -p 7960 FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^live_replicas://p')" = "1" ] && break
  sleep 0.5
done

# The control: the corpus against the NODE, which is what the gate has always
# measured. If this is not clean the run says nothing about the proxy, and
# the failure should be read as a node bug rather than an edge one.
echo "== control: the corpus direct to the node"
NODE=$(./target/release/flint-conformance --target 127.0.0.1:7960 2>&1)
echo "$NODE" | grep -q "(100.0%)" || {
  echo "$NODE" | tail -6 | sed 's/^/  | /'
  echo "FAIL: the corpus is not clean against the NODE — fix that before reading the proxy result"
  exit 1; }
echo "  $(echo "$NODE" | grep '^overall')"

echo "== the corpus through the proxy, authenticated as a tenant, both dialects"
# --yes-flushall because every case FLUSHALLs for a clean keyspace, which
# through a proxy erases the tenant's namespace. `conf` exists for exactly
# that and holds nothing else.
for PROTO in 2 3; do
  ARGS=(--target 127.0.0.1:7962 --auth conf:tok-conf --yes-flushall)
  [ "$PROTO" = 3 ] && ARGS+=(--proto 3)
  OUT=$(./target/release/flint-conformance "${ARGS[@]}" 2>&1)
  # A run that quietly fell back to an unauthenticated connection would be
  # measuring something else entirely, and the pass rate would look the same.
  echo "$OUT" | grep -q "authenticated" || {
    echo "FAIL: the RESP$PROTO run did not authenticate — it did not go through the tenant path"
    exit 1; }
  echo "$OUT" | grep -q "(100.0%)" || {
    echo "$OUT" | tail -8 | sed 's/^/  | /'
    echo "FAIL: the corpus is not clean THROUGH THE PROXY on RESP$PROTO."
    echo "      The node control above passed, so this is the edge: routing, the"
    echo "      RESP re-encode, transaction pinning or the near-cache."
    exit 1; }
  echo "  RESP$PROTO  $(echo "$OUT" | grep '^overall')"
done

echo "PASS: the compatibility corpus is clean through the proxy as a tenant on RESP2 and RESP3 — the path a client actually takes, in the dialect it actually speaks"
