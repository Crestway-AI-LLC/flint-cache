#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Lease drill (ADR-0018): the write lease is held at the CP and renewed by
# the MASTER ITSELF (CPLEASE every ttl/3), not pushed by a controller.
# Proves the two fence triggers, in the order a real failover meets them:
#
#   1. -SUPERSEDED: a fencing record (CPFENCE) committed over a SERVING
#      master fences it within one renewal interval (~ttl/3) — the fast
#      path, and the reason an un-recorded promotion cannot stick.
#   2. CP unreachable: a master that cannot renew fences at TTL — the
#      partition-split-brain guard, exactly ADR-0004's window, held by a
#      quorum instead of one process.
#
# Plus the invariant both share: a self-fence is never auto-undone (no
# resurrection); the way back is FLINTDEMOTE+resync or FLINTPROMOTE.
#
# No controller in this drill AT ALL — that is the point of ADR-0018. Five
# scale runs (8-12) each turned some flavour of controller silence into a
# fleet-wide fence of healthy masters (#168/#171/#172); the lease now
# involves only the master and the CP, so this drill involves only them.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# The scope declared here must COVER every data dir this drill creates
# (BUG-0047). The harness attributes a seat to a drill by this prefix or
# by a declared port; a seat matching neither is unattributable, and the
# one such seat in the suite — failover's zombie, started outside
# fleet.sh's tracking — is what broke the parallel gate.
fleet_init $FLINT_DRILL_ROOT/flint-lease- 6306 6308 6309
fleet_guard
fleet_kill server; fleet_kill controlplane; sleep 0.4
MDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-lease-m.XXXXXX); RDIR=$(mktemp -d $FLINT_DRILL_ROOT/flint-lease-r.XXXXXX)
CPSTATE=$(mktemp -d $FLINT_DRILL_ROOT/flint-lease-cp.XXXXXX)
B=./target/release/flint-server
CP=./target/release/flint-controlplane
CPPORT=6306; MPORT=6308; RPORT=6309
TTL=1500
cleanup() {
  # This used to be `pkill -9 -f "flint-server --port 644"` — a copy-paste
  # from a drill on the 644x ports. This drill runs on 6306-6309, so cleanup
  # matched nothing and every failing run LEAKED its seats. That is how one
  # failure here cascaded: fleet_guard in the next five drills correctly
  # refused to start, seeing Flint processes it did not own, and five drills
  # that were fine reported failure. fleet_kill is scoped to this drill's
  # own ports (fleet_init above), which is the point of using it.
  fleet_kill server
  fleet_kill controlplane
  rm -rf "$MDIR" "$RDIR" "$CPSTATE"
}
trap cleanup EXIT

echo "== CP + pair; both nodes carry --lease-ttl-ms $TTL and dial the CP themselves"
$CP --port $CPPORT --state "$CPSTATE/state" 2>$FLINT_DRILL_ROOT/flint-lease-cp.log &
fleet_wait_ping $CPPORT
# The membership guard: CPLEASE/CPFENCE refuse an address the registry does
# not know, so the pair must be on record before the first renewal matters.
# (A renewal that races this registration is refused and simply retried —
# the deadline stays unarmed until the first success, so nothing fences.)
fleet_cp $CPPORT CPADDPAIR 127.0.0.1:$MPORT,127.0.0.1:$RPORT
$B --port $MPORT --engine rocks --data-dir "$MDIR" \
   --lease-ttl-ms $TTL --journal 127.0.0.1:$CPPORT 2>$FLINT_DRILL_ROOT/flint-lease-m.log &
fleet_wait_listen $MPORT
sleep 0.5
$B --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of 127.0.0.1:$MPORT \
   --lease-ttl-ms $TTL --journal 127.0.0.1:$CPPORT 2>$FLINT_DRILL_ROOT/flint-lease-r.log &
fleet_wait_listen $RPORT
sleep 0.9
cli_ok valkey-cli -p $MPORT SET k v

echo "== master is writable while renewing its own lease (CPLEASE adoption + renewals)"
WRITABLE=0
for _ in $(seq 1 20); do
  [ "$(valkey-cli -p $MPORT SET before ok 2>&1)" = "OK" ] && { WRITABLE=1; break; }
  sleep 0.2
done
[ "$WRITABLE" = 1 ] || { echo "FAIL: master not writable under self-renewed lease"; exit 1; }
# Renewals must actually be landing, or every fence below fires vacuously
# off an unarmed deadline: the CP's counter is the positive control.
sleep 1
RENEWALS=$(valkey-cli -p $CPPORT CPINFO 2>/dev/null | tr -d '\r' | awk -F: '/^lease_renewals_total:/{print $2}')
[ "${RENEWALS:-0}" -ge 1 ] 2>/dev/null \
  || { echo "FAIL: CP saw no CPLEASE renewals (lease_renewals_total=${RENEWALS:-unset}) — the lease is not being managed"; exit 1; }
echo "  writable; CP has served $RENEWALS renewals"

echo "== trigger 1: CPFENCE the replica — the recorded promotion must fence"
echo "   the still-serving master within ~ttl/3, via -SUPERSEDED, not TTL expiry"
FR=$(valkey-cli -p $CPPORT CPFENCE 127.0.0.1:$RPORT 2>&1 | tr -d '\r')
case "$FR" in
  OK*) echo "  fencing record committed: $FR" ;;
  *)   echo "FAIL: CPFENCE was refused: $FR"; exit 1 ;;
esac
FENCED=0
for i in $(seq 1 30); do   # up to 6s; one renewal interval is ttl/3 = 500ms
  RO=$(valkey-cli -p $MPORT SET after bad 2>&1 || true)
  if echo "$RO" | grep -q "READONLY"; then FENCED=1; break; fi
  sleep 0.2
done
[ "$FENCED" = 1 ] || { echo "FAIL: master kept serving writes after a promotion was recorded over it"; exit 1; }
ELAPSED_MS=$(( i * 200 ))
echo "  superseded and fenced after ~${ELAPSED_MS}ms (TTL is ${TTL}ms)"
grep -q "superseded" $FLINT_DRILL_ROOT/flint-lease-m.log 2>/dev/null \
  || { echo "FAIL: the master's log does not say the fence came from -SUPERSEDED — did it merely time out?"; exit 1; }

echo "== the self-fenced master FENCES tenant reads, and is alive rather than down"
# A master that self-fenced cannot know what happened behind the record —
# serving a read risks serving data already superseded. R1
# (crates/flint-server/src/main.rs) covers it because a self-fenced
# ex-master has REPLICA_CONTACT_MS == 0. docs/failover.md is the published
# contract — "reads may -TRYAGAIN then fall back".
R=$(valkey-cli -p $MPORT GET k 2>&1 | tr -d '\r')
case "$R" in
  TRYAGAIN*) echo "  tenant reads fenced: ${R%%;*}" ;;
  v) echo "FAIL: the self-fenced master SERVED a read."
     echo "      It cannot know whether it was superseded, so this is a stale read."
     echo "      Expected -TRYAGAIN (docs/failover.md, R1 fence)."; exit 1 ;;
  *) echo "FAIL: expected -TRYAGAIN from a self-fenced master, got: ${R:-(no reply)}"; exit 1 ;;
esac
# "Fenced" must mean fenced, not dead — asserted against commands that are
# exempt from R1 by design.
[ "$(valkey-cli -p $MPORT PING 2>&1 | tr -d '\r')" = "PONG" ] \
  || { echo "FAIL: the self-fenced master stopped answering PING — that is DOWN, not fenced"; exit 1; }
valkey-cli -p $MPORT FLINTINFO 2>&1 | tr -d '\r' | grep -q '^role:' \
  || { echo "FAIL: FLINTINFO is exempt from the read fence and must still answer"; exit 1; }
echo "  still answering PING and FLINTINFO — fenced, not down"

echo "== complete the recorded promotion: the replica becomes the serving master"
# Baseline the renewal counter NOW: the old master is fenced (read-only
# masters do not renew) and the successor is not yet promoted, so any
# increment from here on is the successor's own renewal loop.
PRE=$(valkey-cli -p $CPPORT CPINFO 2>/dev/null | tr -d '\r' | awk -F: '/^lease_renewals_total:/{print $2}')
# FLINTPROMOTE resets the stale lease deadline (#168) and un-fences the
# node; its own renewal loop takes over against the record CPFENCE wrote.
P=$(valkey-cli -p $RPORT FLINTPROMOTE 0 2 2>&1 | tr -d '\r')
case "$P" in
  OK*|PROMOTED*) : ;;
  *) echo "FAIL: FLINTPROMOTE of the recorded successor failed: $P"; exit 1 ;;
esac
WRITABLE=0
for _ in $(seq 1 30); do
  [ "$(valkey-cli -p $RPORT SET handoff ok 2>&1)" = "OK" ] && { WRITABLE=1; break; }
  sleep 0.2
done
[ "$WRITABLE" = 1 ] || { echo "FAIL: promoted successor is not writable (is its renewal being refused?)"; exit 1; }
# Its deadline is DELIBERATELY unarmed until the first successful renewal
# (FLINTPROMOTE drops the stale one, #168; safety against a rival is the
# -SUPERSEDED record, not the clock). Trigger 2 tests the armed clock, so
# wait for a renewal to land — asserted via the CP's counter, not a sleep.
ARMED=0
for _ in $(seq 1 30); do
  NOW=$(valkey-cli -p $CPPORT CPINFO 2>/dev/null | tr -d '\r' | awk -F: '/^lease_renewals_total:/{print $2}')
  [ "${NOW:-0}" -gt "$PRE" ] 2>/dev/null && { ARMED=1; RENEWALS=$NOW; break; }
  sleep 0.2
done
[ "$ARMED" = 1 ] || { echo "FAIL: the promoted successor never renewed (CP counter stuck at $PRE) — its lease is unmanaged"; exit 1; }
echo "  successor serving and renewing against its own record ($RENEWALS renewals total)"

echo "== trigger 2: kill the CP — the serving master must fence at TTL"
echo "   (it cannot rule out a promotion committed on the other side of the split)"
fleet_kill controlplane
FENCED=0
for i in $(seq 1 40); do   # up to 8s; TTL is 1.5s
  RO=$(valkey-cli -p $RPORT SET after2 bad 2>&1 || true)
  if echo "$RO" | grep -q "READONLY"; then FENCED=1; break; fi
  sleep 0.2
done
[ "$FENCED" = 1 ] || { echo "FAIL: master did not self-fence after losing the CP"; exit 1; }
echo "  self-fenced after ~$(( i * 200 ))ms without a reachable CP (TTL ${TTL}ms)"

echo "== self-fence is NOT auto-undone by a later renewal (no resurrection)"
valkey-cli -p $RPORT FLINTLEASE 5000 >/dev/null
sleep 0.3
RO2=$(valkey-cli -p $RPORT SET after3 bad 2>&1 || true)
echo "$RO2" | grep -q "READONLY" || { echo "FAIL: renewal resurrected a self-fenced master: $RO2"; exit 1; }
echo "stays read-only despite renewal (recovery requires FLINTDEMOTE + resync)"

echo "PASS: CP-held lease fences on -SUPERSEDED within a renewal interval and on CP loss at TTL, with no resurrection (ADR-0018)"
