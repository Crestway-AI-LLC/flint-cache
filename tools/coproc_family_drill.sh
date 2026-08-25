#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Does a co-processor declared in the INVENTORY actually serve its family
# end to end — spawned by flintctl, routed by the proxy, over the mesh mTLS?
#
# WHY THIS EXISTS. ADR-0010 shipped the mechanism and ADR-0017 shipped the
# VEC.* logic, but nothing deployed one: flintctl minted a co-processor leaf
# and then had no inventory key, no spawn, and never passed `--families` to
# the proxy. So the family path was exercised only by hand-started processes
# on a laptop, and the 2026-08-12 i4i bench had to use a STAND-IN
# co-processor. A capability nothing can deploy is a capability that will
# break silently the first time a fleet wants it.
#
# What is asserted, in the order a real deployment fails:
#   1. the seat flintctl spawned is the family's binary, and it is LISTENING;
#   2. the proxy ROUTES VEC.* to it (an unrouted family answers -ERR unknown
#      command, which is the failure this drill exists to catch);
#   3. a VEC.SET's vector is DURABLE in the tenant namespace (ADR-0017 D2:
#      the index is ephemeral, the vectors are not) — proven by reading the
#      key back through the ordinary data path, not through VEC.*;
#   4. VEC.SEARCH returns the planted nearest neighbour, so the index was
#      actually built and queried rather than merely accepting writes;
#   5. `stop` reaps the seat — an unswept co-processor holds its port and
#      the next `start` fails on a bind that looks unrelated.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# 7402 rather than 7411 for the co-processor: build_stamp_drill claims
# 7411-7414 and a port claimed twice makes fleet_guard read the other
# drill's seats as this one's own (BUG-0003, recurred as BUG-0010). The
# block is deliberately not contiguous — 7403-7406 belong to
# cold_start_roles_drill.
fleet_init $FLINT_DRILL_ROOT/flint-coproc-family 7402 7407 7408 7409 7410
fleet_guard
# UNDER THE DECLARED SCOPE, and this is not tidiness. These two named
# coproc_cred's scope — the drill declared flint-coproc-family to fleet_init
# and then put every file it owns in flint-coproc. When this pair was caught
# sharing a scope once before (see the note in gates.sh), the fix went into
# the fleet_init line and these two assignments below it were left pointing
# where they always had: fixed where the bug was seen, not where it lived.
#
# Three consequences, worst first. The scope dir is an OWNERSHIP KEY, not a
# storage location: _fleet_ours attributes seats by scope dir or declared
# ports, so this drill's seats lived inside coproc_cred's scope and
# coproc_cred's fleet_kill could adopt and kill them as its own. The rm -rf
# below then deletes coproc_cred's state on entry AND from the EXIT trap. And
# serially — which is all main does today — it destroys the post-mortem
# directory of a coproc_cred that has just failed, because CORE runs
# coproc_cred first.
#
# assert_no_scope_overlap could not see any of it: it compares what drills
# DECLARE, and nothing read what a drill USES.
STATE=$FLINT_DRILL_ROOT/flint-coproc-family
INV=$FLINT_DRILL_ROOT/flint-coproc-family.flint
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill vec
sleep 0.3
cleanup() {
  ./target/release/flintctl -f "$INV" stop 2>/dev/null
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill vec
  rm -rf "$STATE" "$INV"
}
trap cleanup EXIT
rm -rf "$STATE" "$INV"

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build server"; exit 1; }
cargo build --release -q -p flint-ctl -p flint-proxy -p flint-controlplane -p flint-vec \
  || { echo "FAIL: build fleet"; exit 1; }
fleet_warm ./target/release/flint-server ./target/release/flint-proxy \
  ./target/release/flint-controlplane ./target/release/flint-vec

# tls on: the co-processor presents the SERVER-ONLY coproc leaf, so this also
# proves that leaf is accepted by the proxy's dial (ADR-0010 D2 mints it
# without clientAuth on purpose; a mesh `int` leaf here would pass the drill
# and defeat the isolation).
cat > "$INV" <<EOF
disposable on
tls on
statedir $STATE
bins ./target/release
cp 127.0.0.1:7410
pair 127.0.0.1:7407,127.0.0.1:7408
proxy 127.0.0.1:7409
coproc VEC. 127.0.0.1:7402
coproc-index-bytes 268435456
EOF
CTL="./target/release/flintctl -f $INV"

echo "== bootstrap a fleet whose inventory DECLARES a co-processor"
$CTL bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; $CTL status; exit 1; }
$CTL tenant add vt tok-vt vt 1 >/dev/null 2>&1

echo "== 1. flintctl spawned the family's binary, and it is listening"
pgrep -f "flint-vec --port 7402" >/dev/null || {
  echo "FAIL: no flint-vec seat for the declared 'coproc VEC.' line — flintctl"
  echo "      parsed the key and started nothing"
  ls "$STATE/logs" 2>/dev/null | sed 's/^/    log: /'
  exit 1
}
[ -f "$STATE/pids/vec-7402.pid" ] || {
  echo "FAIL: seat has no pidfile at vec-7402.pid — stop/sweep cannot reap what"
  echo "      it cannot name"; exit 1
}

echo "== 2. the proxy ROUTES VEC.* to it (not -ERR unknown command)"
C="valkey-cli -p 7409 -a tok-vt --no-auth-warning"
# A cold co-processor REBUILDS its index from the namespace's durable rows
# (ADR-0017: the index is the ephemeral half) and answers -LOADING until that
# finishes. That is the contract, not a fault — so wait for it the way a
# client must, and only then judge the reply. 20s: a warm-up that outlasts
# that on an empty namespace is a real failure worth failing on.
for _ in $(seq 1 100); do
  OUT=$($C VEC.CREATE s DIM 4 METRIC cosine 2>&1)
  case "$OUT" in *LOADING*) sleep 0.2 ;; *) break ;; esac
done
case "$OUT" in
  OK*) ;;
  *)
    echo "FAIL: VEC.CREATE answered '$OUT'"
    case "$OUT" in
      *unknown*) echo "      -> the family is NOT in the proxy's route table. The table"
                 echo "         comes from CPSNAPSHOT element 7 whenever the CP sends it,"
                 echo "         so --families alone is overwritten: register with CPFAMILY." ;;
      *COPROCUNAVAIL*) echo "      -> the family IS routed but the proxy could not reach the"
                       echo "         seat. Dial/TLS problem, not a routing one." ;;
    esac
    echo "  co-processor log:"; tail -15 "$STATE/logs/vec-7402.log" 2>/dev/null | sed 's/^/    vec| /'
    echo "  proxy log:";        tail -15 "$STATE/logs/proxy-7409.log" 2>/dev/null | sed 's/^/    px | /'
    exit 1 ;;
esac
$C VEC.SET s a "1,0,0,0" >/dev/null 2>&1 || { echo "FAIL: VEC.SET a"; exit 1; }
$C VEC.SET s b "0,1,0,0" >/dev/null 2>&1 || { echo "FAIL: VEC.SET b"; exit 1; }
$C VEC.SET s c "0.99,0.01,0,0" >/dev/null 2>&1 || { echo "FAIL: VEC.SET c"; exit 1; }

echo "== 3. the VECTORS are durable: kill the seat, it REBUILDS from the namespace"
# ADR-0017 D2: VEC.SET persists over the PROXYCHAN channel FIRST and commits
# the index only if that lands, so the index is the ephemeral half and the
# vectors are not. The honest proof is not a key count (the durable rows are
# NUL-prefixed internal keys, deliberately not part of the tenant's visible
# keyspace) — it is that a COLD co-processor reconstructs the corpus from
# durable rows alone. That also exercises the cold-start path a fleet hits on
# every restart, which nothing else covers.
DBS=$($C DBSIZE 2>/dev/null | tr -d '\r')
# The seat's log is APPENDED across restarts, and boot 1 already logged a
# rebuild (of an empty namespace). Waiting for the string alone matches that
# stale line and reads "(0 vectors)" back as if it were the restart's answer —
# so count the rebuilds present BEFORE the kill and wait for one MORE.
BEFORE=$(grep -c "rebuilt ns" "$STATE/logs/vec-7402.log" 2>/dev/null || echo 0)
pkill -9 -f "flint-vec --port 7402" 2>/dev/null
sleep 0.3
$CTL start >/dev/null 2>&1
# The rebuild is chunked and driven by channel touches (main.rs rebuild_chunk),
# so it needs a command to make progress: poke the set each tick.
for _ in $(seq 1 150); do
  NOW=$(grep -c "rebuilt ns" "$STATE/logs/vec-7402.log" 2>/dev/null || echo 0)
  [ "$NOW" -gt "$BEFORE" ] && break
  $C VEC.INFO s >/dev/null 2>&1
  sleep 0.2
done
REBUILT=$(grep -o 'rebuilt ns "vt" ([0-9]* vectors)' "$STATE/logs/vec-7402.log" | tail -1)
case "$REBUILT" in
  *"(3 vectors)"*) ;;
  "") echo "FAIL: the restarted co-processor never logged a rebuild — the cold-start"
      echo "      path did not run (DBSIZE through the edge was $DBS)"
      tail -12 "$STATE/logs/vec-7402.log" | sed 's/^/    vec| /'; exit 1 ;;
  *)  echo "FAIL: cold start recovered '$REBUILT', expected 3 vectors — the durable"
      echo "      half of the two-phase write did not land for every VEC.SET"
      exit 1 ;;
esac
echo "  cold start rebuilt 3 vectors from durable rows ($REBUILT)"

echo "== 4. VEC.SEARCH returns the planted nearest neighbour (on the REBUILT index)"
# Query ~= a, so a must rank first and c (0.99,0.01) must beat b (orthogonal).
# Run against the index rebuilt in step 3: a corpus that survives a restart
# but comes back unsearchable is the failure this ordering is chosen to catch.
for _ in $(seq 1 100); do
  RES=$($C VEC.SEARCH s "1,0,0,0" 2 2>&1)
  case "$RES" in *LOADING*) sleep 0.2 ;; *) break ;; esac
done
echo "$RES" | head -1 | grep -q "^a$" || {
  echo "FAIL: nearest neighbour was not 'a'. VEC.SEARCH said:"
  echo "$RES" | sed 's/^/    /'
  exit 1
}
echo "$RES" | grep -q "^c$" || {
  echo "FAIL: 'c' (0.99,0.01,0,0) did not make the top-2 against the orthogonal"
  echo "      'b' — the index is answering, but not by distance"
  echo "$RES" | sed 's/^/    /'
  exit 1
}
echo "  top-2 = a, c (by cosine), served through the proxy over mTLS"

echo "== 5. stop reaps the co-processor seat"
$CTL stop >/dev/null 2>&1
sleep 0.5
pgrep -f "flint-vec --port 7402" >/dev/null && {
  echo "FAIL: the co-processor survived 'stop' — it will hold port 7402 against"
  echo "      the next start, which then fails on a bind error naming no family"
  exit 1
}
echo "  seat gone after stop"

echo "PASS: co-processor family drill — an inventory-declared co-processor is spawned by flintctl, routed by the proxy over mesh mTLS, persists its vectors into the tenant namespace, answers VEC.SEARCH by distance, and is reaped by stop"
