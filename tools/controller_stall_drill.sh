#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A controller stall must cost NOTHING but failover capability (ADR-0018).
#
# THIS DRILL IS THE INVERSE OF WHAT IT WAS. Under ADR-0004 the controller
# renewed every master's lease, so a stall longer than --lease-ttl-ms fenced
# EVERY pair fleet-wide (#168) — and this drill asserted the fleet recovered
# from that fence. Five scale runs (8-12) showed the shape was structural:
# each mode of controller silence (#168 stall, #171 replica-less gate, #172
# post-bringup silence) became a fleet-wide write outage, because the fence
# was anchored to the least available component in the system.
#
# ADR-0018 moved the lease to the CP: masters renew THEMSELVES (CPLEASE
# every ttl/3 against the 3-seat CP), and the controller only commits the
# fencing record (CPFENCE) before promoting. A stalled controller therefore
# renews nothing and fences nobody. So the drill now asserts the fleet does
# NOT fence during the stall and keeps serving writes throughout —
# the exact scenario that killed run 12's fleet permanently.
#
# POSITIVE CONTROL, because "nothing happened" is the easiest thing in the
# world to assert vacuously: after the stall, stop the CP past the TTL and
# every master must fence — same mechanism, correct anchor. This also proves
# renewals were landing all along (an unmanaged lease never arms the
# deadline, so it could not fence here). Then resume the CP and the
# controller must recover both pairs (CPFENCE + promote, end to end).
#
# SIGSTOP is the honest stand-in for what actually happens in the field: a
# GC pause, CPU starvation on the controller host, or a poll loop that
# overruns under load. It is deterministic and needs no fault injection.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-stall 7371 7372 7373 7374 7375 7376
fleet_guard
D=$FLINT_DRILL_ROOT/flint-stall; INV=$D/cluster.flint
CTL=./target/release/flintctl
LEASE="${FLINT_LEASE_TTL_MS:-3000}"
STALL_S="${FLINT_STALL_S:-10}"
fleet_kill server; fleet_kill proxy; fleet_kill controlplane; fleet_kill controller
sleep 0.4
cleanup() {
  # CONT first: a still-STOPPED process cannot be asked to stop cleanly.
  pkill -CONT -f 'flint-[c]ontroller' 2>/dev/null
  pkill -CONT -f 'flint-[c]ontrolplane' 2>/dev/null
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
# Show what failed. A drill that swallows its setup error costs a debugging
# round trip every time it trips, which is the whole lesson of #168's harness.
$CTL -f "$INV" bootstrap >"$D/bootstrap.log" 2>&1 \
  || { echo "FAIL: bootstrap"; tail -20 "$D/bootstrap.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" verify >"$D/verify.log" 2>&1 \
  || { echo "FAIL: fleet did not verify before the stall"; tail -15 "$D/verify.log" | sed 's/^/  | /'; exit 1; }
$CTL -f "$INV" tenant add stall tok-stall stall 1 >/dev/null 2>&1 \
  || { echo "FAIL: could not create the tenant"; exit 1; }

masters() { $CTL -f "$INV" status 2>/dev/null | awk '$1=="pair" && $4=="master"' | wc -l | tr -d ' '; }
edge_set() { valkey-cli -h 127.0.0.1 -p 7375 -a tok-stall --no-auth-warning SET stall:k ok 2>&1 | tr -d '\r' | head -1; }
pairs() { $CTL -f "$INV" status 2>/dev/null | grep -E '^pair' | sed 's/^/    /'; }

[ "$(masters)" = 2 ] || { echo "FAIL: expected 2 masters before the stall"; pairs; exit 1; }
[ "$(edge_set)" = OK ] || { echo "FAIL: edge not writable before the stall"; exit 1; }
echo "  baseline: 2/2 masters, edge writable"

# MATCH THE PROCESS, NOT A MENTION OF ITS NAME.
#
# Two traps, and this line has now been caught by both. The BRACKET stops the
# pattern text matching its own command line — a `pkill -f 'flint-controller'`
# in someone's terminal history was enough to pick the calling shell, so the
# SIGSTOP froze the wrong process and the assertion tested nothing.
#
# The bracket does NOT stop a match on a process that merely NAMES the binary.
# An editor, a grep, or an agent opened on `crates/flint-controller/src` has
# that string in its own command line, so `flint-[c]ontroller` counted it and
# this drill failed with "found 3" while printing one. The count and the
# listing below disagreed for the whole time — the listing already had the
# trailing space that saved it, and the count did not.
#
# So anchor on the name being the END of an argument: an executable path ends
# there, a directory path (`.../flint-controller/src`) does not.
CTL_PAT='flint-[c]ontroller( |$)'
CPID=$(pgrep -f "$CTL_PAT" | head -1)
[ -n "$CPID" ] || { echo "FAIL: no controller process to stall"; exit 1; }
# `pgrep -c` is a Linux extension; macOS pgrep has no such flag and prints a
# usage error to stderr while yielding an empty count. Pipe to wc -l instead so
# the drill counts the same way on the dev laptop and in CI.
NCTL=$(pgrep -f "$CTL_PAT" | wc -l | tr -cd '0-9')
[ "${NCTL:-0}" = 1 ] || { echo "FAIL: expected exactly 1 controller, found ${NCTL:-0} — stalling one of several proves nothing"; pgrep -fl "$CTL_PAT" | sed 's/^/    /'; exit 1; }
echo "== SIGSTOP the controller (pid $CPID) for ${STALL_S}s — lease is ${LEASE}ms, held at the CP"
kill -STOP "$CPID"
# Prove the stop took: a SIGSTOP that silently did nothing would make the
# no-fence assertion below meaningless.
sleep 0.5
CSTAT=$(ps -o stat= -p "$CPID" 2>/dev/null | tr -d ' ')
case "$CSTAT" in
  T*) echo "  controller is stopped (state $CSTAT)" ;;
  *)  echo "FAIL: controller pid $CPID is in state '${CSTAT:-gone}', not stopped — the stall never happened"; exit 1 ;;
esac

# THE INVERTED ASSERTION: every second of the stall, both masters stand and
# the edge serves writes. One fence, one refused write, and ADR-0018 has
# regressed to the controller-anchored lease this drill used to document.
for s in $(seq 1 "$STALL_S"); do
  M=$(masters)
  [ "$M" = 2 ] || {
    echo "FAIL: ${s}s into the controller stall the fleet has $M/2 masters —"
    echo "      a stalled controller fenced a healthy master (#168 is back)."
    pairs; exit 1
  }
  W=$(edge_set)
  [ "$W" = OK ] || {
    echo "FAIL: ${s}s into the controller stall an edge write returned '$W' —"
    echo "      writes must not depend on the controller being scheduled."
    pairs; exit 1
  }
  sleep 1
done
echo "  ${STALL_S}s stalled: 2/2 masters, every edge write OK — nothing fenced"

echo "== SIGCONT — nothing to recover; the fleet must simply still verify"
kill -CONT "$CPID"
sleep 1
[ "$(masters)" = 2 ] || { echo "FAIL: masters lost right after the controller resumed"; pairs; exit 1; }
$CTL -f "$INV" verify >/dev/null 2>&1 \
  || { echo "FAIL: fleet does not verify after the stall"; $CTL -f "$INV" verify 2>&1 | tail -12 | sed 's/^/  /'; exit 1; }

# POSITIVE CONTROL: the fence must still exist, anchored where ADR-0018 put
# it. Stop the CP past the TTL: masters that cannot renew must fence...
# Bracketed AND end-anchored, for both reasons given at CTL_PAT above.
CP_PAT='flint-[c]ontrolplane( |$)'
CPPID=$(pgrep -f "$CP_PAT" | head -1)
[ -n "$CPPID" ] || { echo "FAIL: no control-plane process for the positive control"; exit 1; }
NCP=$(pgrep -f "$CP_PAT" | wc -l | tr -cd '0-9')
[ "${NCP:-0}" = 1 ] || { echo "FAIL: expected exactly 1 CP seat, found ${NCP:-0}"; pgrep -fl "$CP_PAT" | sed 's/^/    /'; exit 1; }
echo "== positive control: SIGSTOP the CP (pid $CPPID) — masters must fence at TTL"
kill -STOP "$CPPID"
FENCED=0
for _ in $(seq 1 15); do
  [ "$(masters)" = 0 ] && { FENCED=1; break; }
  sleep 1
done
if [ "$FENCED" != 1 ]; then
  kill -CONT "$CPPID"
  echo "FAIL: masters kept serving with the CP stopped past the TTL —"
  echo "      the lease is not being enforced at all, so the no-fence result"
  echo "      above was vacuous."
  pairs; exit 1
fi
echo "  fenced as designed: 0/2 masters without a reachable CP"

# ...and once the CP is back, the CONTROLLER recovers the fleet — the full
# ADR-0018 promotion path (CPFENCE committed, then FLINTPROMOTE) end to end.
echo "== SIGCONT the CP — the controller must recover both pairs"
kill -CONT "$CPPID"
HEAL=""
for i in $(seq 1 45); do
  if [ "$(masters)" = 2 ] && [ "$(edge_set)" = OK ]; then HEAL=$i; break; fi
  sleep 1
done
[ -n "$HEAL" ] || {
  echo "FAIL: 45s after the CP resumed the fleet has $(masters)/2 masters and edge_set=$(edge_set)"
  pairs
  L=$(ls -t "$D"/state/logs/*ontroller* 2>/dev/null | head -1)
  [ -n "$L" ] && { echo "  --- controller log (tail 25) ---"; tail -25 "$L" | sed 's/^/  | /'; }
  exit 1
}
echo "  recovered ${HEAL}s after the CP resumed"
pairs
for _ in $(seq 1 30); do
  $CTL -f "$INV" verify >/dev/null 2>&1 && break
  sleep 1
done
$CTL -f "$INV" verify >/dev/null 2>&1 \
  || { echo "FAIL: fleet does not verify after the recovery"; $CTL -f "$INV" verify 2>&1 | tail -12 | sed 's/^/  /'; exit 1; }

echo "PASS: a ${STALL_S}s controller stall fences NOTHING (writes served throughout); the fence still fires on CP loss and the controller recovers the fleet (ADR-0018)"
