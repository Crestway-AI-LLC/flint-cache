#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Lease drill: a master lease-managed by a controller self-fences when
# renewals stop (the controller can no longer reach it). Proves the master
# stops accepting writes on TTL expiry WITHOUT anyone sending FLINTDEMOTE —
# the partition-split-brain guard.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init /tmp/flint-lease-m 6308 6309
fleet_guard
fleet_kill server; fleet_kill controller; sleep 0.4
MDIR=$(mktemp -d /tmp/flint-lease-m.XXXXXX); RDIR=$(mktemp -d /tmp/flint-lease-r.XXXXXX)
B=./target/release/flint-server
MPORT=6308; RPORT=6309
cleanup() {
  # This used to be `pkill -9 -f "flint-server --port 644"` — a copy-paste
  # from a drill on the 644x ports. This drill runs on 6308/6309, so cleanup
  # matched nothing and every failing run LEAKED both seats. That is how one
  # failure here cascaded: fleet_guard in the next five drills correctly
  # refused to start, seeing Flint processes it did not own, and five drills
  # that were fine reported failure. fleet_kill is scoped to this drill's
  # own ports (fleet_init above), which is the point of using it.
  fleet_kill server
  fleet_kill controller
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

$B --port $MPORT --engine rocks --data-dir "$MDIR" 2>/dev/null &
fleet_wait_listen $MPORT
sleep 0.5
$B --port $RPORT --engine rocks --data-dir "$RDIR" --replica-of 127.0.0.1:$MPORT 2>/dev/null &
fleet_wait_listen $RPORT
sleep 0.9
cli_ok valkey-cli -p $MPORT SET k v

echo "== controller manages the master with a 1.5s lease TTL"
./target/release/flint-controller --nodes 127.0.0.1:$MPORT,127.0.0.1:$RPORT --id L \
  --poll-ms 150 --lease-ttl-ms 1500 2>/tmp/flint-lease.log &
sleep 1.0

echo "== master is writable while the lease is renewed"
[ "$(valkey-cli -p $MPORT SET before ok 2>&1)" = "OK" ] || { echo "FAIL: master not writable under lease"; exit 1; }
echo "writable OK"

echo "== stop renewals (kill the controller — simulates the master being"
echo "   partitioned from every controller); master must self-fence on TTL"
fleet_kill controller
FENCED=0
for i in $(seq 1 40); do   # up to 8s; TTL is 1.5s
  RO=$(valkey-cli -p $MPORT SET after bad 2>&1 || true)
  if echo "$RO" | grep -q "READONLY"; then FENCED=1; break; fi
  sleep 0.2
done
[ "$FENCED" = "1" ] || { echo "FAIL: master did not self-fence after lease expiry"; exit 1; }
echo "self-fenced after ~$(( i * 200 ))ms of no renewal (TTL 1500ms)"
grep -q "self-fenced" "$MDIR.log" 2>/dev/null && echo "  (server logged the self-fence)"

echo "== the self-fenced master FENCES tenant reads, and is alive rather than down"
# This assertion used to be `GET k` = "v", with the heading "data still
# readable on the self-fenced master". That was true when the self-fence
# landed (97b1426, 2026-07-12) and stopped being true five days later when
# the R1 stale-read fence landed (9b4f685, 2026-07-17). It has been failing
# deterministically ever since, and nothing caught it because this drill is
# not in gates.sh CORE.
#
# The SERVER is right and the old assertion was wrong. A master that
# self-fenced has lost contact with every controller, so it cannot know
# whether a replica was promoted behind the partition — serving a read would
# risk serving data that has already been superseded. R1
# (crates/flint-server/src/main.rs:1402) covers this node because a
# self-fenced ex-master has REPLICA_CONTACT_MS == 0: it was never a replica,
# and will not have contact until it is demoted and resynced. docs/failover.md:315
# is the published contract — "reads may -TRYAGAIN then fall back".
R=$(valkey-cli -p $MPORT GET k 2>&1 | tr -d '\r')
case "$R" in
  TRYAGAIN*) echo "  tenant reads fenced: ${R%%;*}" ;;
  v) echo "FAIL: the self-fenced master SERVED a read."
     echo "      It cannot know whether it was superseded, so this is a stale read."
     echo "      Expected -TRYAGAIN (docs/failover.md:315, R1 fence)."; exit 1 ;;
  *) echo "FAIL: expected -TRYAGAIN from a self-fenced master, got: ${R:-(no reply)}"; exit 1 ;;
esac
# "Fenced" must mean fenced, not dead — the distinction the old heading was
# reaching for, asserted against commands that are exempt from R1 by design.
[ "$(valkey-cli -p $MPORT PING 2>&1 | tr -d '\r')" = "PONG" ] \
  || { echo "FAIL: the self-fenced master stopped answering PING — that is DOWN, not fenced"; exit 1; }
valkey-cli -p $MPORT FLINTINFO 2>&1 | tr -d '\r' | grep -q '^role:' \
  || { echo "FAIL: FLINTINFO is exempt from the read fence and must still answer"; exit 1; }
echo "  still answering PING and FLINTINFO — fenced, not down"

echo "== self-fence is NOT auto-undone by a later renewal (no resurrection)"
valkey-cli -p $MPORT FLINTLEASE 5000 >/dev/null
sleep 0.3
RO2=$(valkey-cli -p $MPORT SET after2 bad 2>&1 || true)
echo "$RO2" | grep -q "READONLY" || { echo "FAIL: renewal resurrected a self-fenced master: $RO2"; exit 1; }
echo "stays read-only despite renewal (recovery requires FLINTDEMOTE + resync)"

echo "PASS: lease self-fencing closes the partition window without reaching the node"
