#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A controller stall must not be a permanent fleet-wide write outage (#168).
#
# THE BUG THIS PINS. flint-server flips a master to read-only when its lease
# goes unrenewed, and FLINTINFO renders `role:` FROM that read-only flag — so a
# controller stalled longer than --lease-ttl-ms leaves EVERY pair with no
# master-CLAIMER although nothing died and nothing diverged. That state used to
# be self-perpetuating: the controller's degraded-window gate only promotes a
# pair that converged recently, `last_converged` only advances while a
# legitimate master exists, so no master => no convergence => the gate refused
# forever. Measured on this exact fleet before the fix: a 10s stall took it to
# zero masters and it was STILL at zero 45s after the controller came back.
# Found by the S2.2 scale test, where ~20k SET/s died after 110s and never
# recovered.
#
# The fix is recovery, NOT weaker fencing: the controller now recognises a
# self-fenced master that still serves a caught-up replica (top epoch,
# live_replicas>=1, seq_lag==0) as the in-sync lineage holder and promotes it,
# and FLINTPROMOTE drops the stale pre-promotion lease deadline so the watchdog
# cannot immediately re-fence the node it just recovered.
#
# SIGSTOP is the honest stand-in for what actually happens in the field: a GC
# pause, CPU starvation on the controller host, or a poll loop that overruns
# the lease under load. It is deterministic and needs no fault injection.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-stall 7371 7372 7373 7374 7375 7376
fleet_guard
D=/tmp/flint-stall; INV=$D/cluster.flint
CTL=./target/release/flintctl
LEASE="${FLINT_LEASE_TTL_MS:-3000}"
STALL_S="${FLINT_STALL_S:-10}"
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  # CONT first: a still-STOPPED controller cannot be asked to stop cleanly.
  pkill -CONT -f flint-controller 2>/dev/null
  $CTL -f "$INV" stop >/dev/null 2>&1
  fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
  -p flint-controller -p flint-ctl --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat > "$INV" <<EOF
disposable on
statedir $D/state
bins ./target/release
tls on
cp 127.0.0.1:7376
pair 127.0.0.1:7371,127.0.0.1:7372
pair 127.0.0.1:7373,127.0.0.1:7374
proxy 127.0.0.1:7375
controller on
poll-ms 150
confirm 3
lease-ttl-ms $LEASE
EOF

echo "== bootstrap (CP, 2 pairs, proxy, controller; lease-ttl-ms=$LEASE)"
$CTL -f "$INV" bootstrap >/dev/null 2>&1 || { echo "FAIL: bootstrap"; exit 1; }
$CTL -f "$INV" verify   >/dev/null 2>&1 || { echo "FAIL: fleet did not verify before the stall"; exit 1; }
$CTL -f "$INV" tenant add stall tok-stall stall 1 >/dev/null 2>&1 \
  || { echo "FAIL: could not create the tenant"; exit 1; }

masters() { $CTL -f "$INV" status 2>/dev/null | awk '$1=="pair" && $4=="master"' | wc -l | tr -d ' '; }
edge_set() { valkey-cli -h 127.0.0.1 -p 7375 -a tok-stall --no-auth-warning SET stall:k ok 2>&1 | tr -d '\r' | head -1; }
pairs() { $CTL -f "$INV" status 2>/dev/null | grep -E '^pair' | sed 's/^/    /'; }

[ "$(masters)" = 2 ] || { echo "FAIL: expected 2 masters before the stall"; pairs; exit 1; }
[ "$(edge_set)" = OK ] || { echo "FAIL: edge not writable before the stall"; exit 1; }
echo "  baseline: 2/2 masters, edge writable"

CPID=$(pgrep -f 'flint-controller' | head -1)
[ -n "$CPID" ] || { echo "FAIL: no controller process to stall"; exit 1; }
echo "== SIGSTOP the controller (pid $CPID) for ${STALL_S}s — lease is ${LEASE}ms"
kill -STOP "$CPID"
# POSITIVE CONTROL. Assert the fence actually happened: if a future change
# stopped leases expiring, the recovery assertion below would pass vacuously
# and this drill would guard nothing.
FENCED=0
for _ in $(seq 1 "$STALL_S"); do
  [ "$(masters)" = 0 ] && { FENCED=1; break; }
  sleep 1
done
[ "$FENCED" = 1 ] || {
  echo "FAIL: the fleet never self-fenced during a ${STALL_S}s stall (lease=${LEASE}ms) —"
  echo "      this drill asserts RECOVERY from the fence, so without the fence it proves nothing."
  pairs; exit 1
}
echo "  fenced as designed: 0/2 masters while the controller is stalled"

echo "== SIGCONT — the fleet must recover ITSELF"
kill -CONT "$CPID"
HEAL=""
for i in $(seq 1 30); do
  if [ "$(masters)" -ge 2 ] 2>/dev/null && [ "$(edge_set)" = OK ]; then HEAL=$i; break; fi
  sleep 1
done
[ -n "$HEAL" ] || {
  echo "FAIL: 30s after the controller resumed the fleet still has $(masters)/2 masters and edge_set=$(edge_set)"
  echo "      — a controller stall is once again a PERMANENT write outage (#168)."
  pairs; exit 1
}
echo "  recovered ${HEAL}s after the controller resumed"
pairs

# The pair must be whole again, not merely writable: a promotion that left the
# replica behind would still answer SET while carrying a single copy.
for _ in $(seq 1 30); do
  $CTL -f "$INV" verify >/dev/null 2>&1 && break
  sleep 1
done
$CTL -f "$INV" verify >/dev/null 2>&1 \
  || { echo "FAIL: fleet does not verify after recovering from the stall"; $CTL -f "$INV" verify 2>&1 | tail -12 | sed 's/^/  /'; exit 1; }

echo "PASS: a ${STALL_S}s controller stall fences the fleet, and the fleet recovers itself within ${HEAL}s and still verifies (#168)"
