#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A node that is coming up must be VISIBLE, not invisible (#176).
#
# WHY. A fresh replica downloads its entire dataset before it can serve, and
# until this fix its listener stayed CLOSED for that whole window — minutes on
# a fleet, tens of minutes at scale. That is not "looks unhealthy": at the TCP
# layer a syncing node is indistinguishable from a dead one, which is the
# strongest wrong signal available, and three separate components acted on it.
# `flintctl start` read the seat as absent and WIPED it to respawn (#139: four
# restarts in four minutes on the playground, never converging). The
# controller could not tell it from a corpse (#189). `verify` called the pair
# single-copy (#136's shape).
#
# So the listener now opens BEFORE the sync and answers in the state Redis
# already defined for exactly this: LOADING. This drill proves the whole
# contract over the wire — the node answers, says what it is, refuses what it
# cannot do, and then converges.
#
# HOW IT MAKES THE WINDOW OBSERVABLE — a ratio, not a volume. The loading
# state is only observable while a sync is in flight, so instead of seeding
# gigabytes this caps the MASTER'S full-sync serve rate (--fullsync-rate-bytes,
# the shipped knob) and seeds a few tens of MB. Same regime, seconds instead of
# an hour.
#
# THE POSITIVE CONTROL IS THE POINT. If the sync completes before the first
# sample, every assertion below would be vacuous and the drill would report a
# pass it did not earn — the #121 failure shape. So `loading:1` must actually
# be OBSERVED, and the drill fails saying so if it never was.
#
# Requires a release build with --features rocks, valkey-cli and python3.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-loading 6463 6464
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-loading; rm -rf "$D"; mkdir -p "$D"
MPORT=6463; RPORT=6464
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }
fleet_warm ./target/release/flint-server

CAP=$((4 * 1024 * 1024))   # 4 MiB/s serve cap: turns ~24MB into ~6s of sync

echo "== master holding a dataset worth streaming, serving it at $((CAP / 1024 / 1024)) MiB/s"
$B --port $MPORT --engine rocks --data-dir "$D/m" --fullsync-rate-bytes $CAP \
  >"$D/m.log" 2>&1 &
fleet_wait_ping $MPORT

# INCOMPRESSIBLE values. Repeated text would compress on disk and over the
# wire, so the transfer would be a fraction of what the drill seeded and the
# loading window would close before the first sample — the failure mode is
# silent (#169, same mistake).
python3 - <<PY
import socket, os
def resp(a):
    return b"*%d\r\n" % len(a) + b"".join(b"\$%d\r\n%s\r\n" % (len(x), x) for x in a)
s = socket.create_connection(("127.0.0.1", $MPORT), timeout=30); s.settimeout(30)
for i in range(3000):
    s.sendall(resp([b"SET", b"ld:%d" % i, os.urandom(8192)])); s.recv(64)
s.sendall(resp([b"FLINTSNAPSHOT", b"$D/snap"])); s.recv(256)   # memtables -> SSTs
PY
SST=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r' | sed -n 's/^sst_bytes://p')
echo "  master holds $SST bytes of SSTs"
[ "${SST:-0}" -gt $((8 * 1024 * 1024)) ] \
  || { echo "FAIL: dataset too small to keep a sync in flight ($SST bytes)"; exit 1; }

echo "== a master reports itself SERVING, not loading"
# Discrimination first: if every node answered `loading:1` the assertions
# below would pass against a server that simply never leaves the state.
MINFO=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r')
echo "$MINFO" | grep -qx 'loading:0' \
  || { echo "FAIL: a serving master must report loading:0"; echo "$MINFO" | head -3; exit 1; }
echo "$MINFO" | grep -qx 'role:master' \
  || { echo "FAIL: master does not claim role:master"; exit 1; }

echo "== fresh replica: the port must answer from INSIDE the initial full sync"
T0=$(python3 -c 'import time; print(time.time())')
$B --port $RPORT --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:$MPORT \
  >"$D/r.log" 2>&1 &
fleet_wait_ping $RPORT
T_PONG=$(python3 -c 'import time; print(time.time())')

# Sample the loading state. Everything asserted here is asserted on a node
# that has NOT finished syncing.
SEEN=0; PROBE=""
for _ in $(seq 1 400); do
  RINFO=$(valkey-cli -p $RPORT FLINTINFO 2>/dev/null | tr -d '\r')
  echo "$RINFO" | grep -qx 'loading:1' || { sleep 0.05; continue; }
  SEEN=1
  echo "$RINFO" | grep -qx 'role:loading' \
    || { echo "FAIL: loading node must not claim a serving role"; echo "$RINFO"; exit 1; }
  echo "$RINFO" | grep -q '^loading_ms:' \
    || { echo "FAIL: no loading_ms — progress must be observable, not just asserted"; exit 1; }
  # Data commands: refused with the code every mainstream client retries on.
  for CMD in "GET ld:1" "SET ld:1 nope" "FLINTPROMOTE 1"; do
    R=$(valkey-cli -p $RPORT $CMD 2>&1)
    case "$R" in *LOADING*) ;; *) echo "FAIL: '$CMD' answered [$R], not -LOADING"; exit 1;; esac
  done
  # FLINTNS is the one that keeps the fleet safe with no proxy change at all:
  # the proxy pins every backend connection to a namespace before any data
  # command can travel on it, so refusing the pin drops a syncing node into
  # the existing dead-backend path instead of leaking -LOADING to a tenant.
  R=$(valkey-cli -p $RPORT FLINTNS acme 2>&1)
  case "$R" in *LOADING*) ;; *) echo "FAIL: FLINTNS answered [$R], not -LOADING"; exit 1;; esac
  PROBE=$RINFO
  break
done
[ "$SEEN" = "1" ] || {
  echo "FAIL: never observed loading:1 — the sync finished before the first sample, so"
  echo "      every assertion in this drill would have been vacuous. Lower CAP or seed"
  echo "      more, rather than accepting a pass the run did not earn."
  exit 1
}
echo "  answered PING and refused data commands at $(echo "$PROBE" | sed -n 's/^loading_ms://p')ms into the sync"

echo "== and the master must not count it as a live replica while it seeds"
LR=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r' | sed -n 's/^live_replicas://p')
[ "${LR:-1}" = "0" ] \
  || { echo "FAIL: master counts a still-seeding node as a live replica ($LR)"; exit 1; }
echo "  master: live_replicas 0"

echo "== then it converges, and says so"
fleet_wait_ready $RPORT
T_READY=$(python3 -c 'import time; print(time.time())')
RINFO=$(valkey-cli -p $RPORT FLINTINFO | tr -d '\r')
echo "$RINFO" | grep -qx 'loading:0' || { echo "FAIL: still loading after ready"; exit 1; }
echo "$RINFO" | grep -qx 'role:replica' \
  || { echo "FAIL: converged node is not a replica"; echo "$RINFO" | head -3; exit 1; }
[ "$(valkey-cli -p $RPORT GET ld:1 | wc -c | tr -d ' ')" \
   = "$(valkey-cli -p $MPORT GET ld:1 | wc -c | tr -d ' ')" ] \
  || { echo "FAIL: replica did not converge to the master's data"; exit 1; }

# THE MEASUREMENT THAT MAKES THE FIX MEAN SOMETHING: the port answers long
# before the node serves. Anything that waits on PONG where it means ready
# returns in the first gap and gets a node that refuses every data command —
# which is why flintctl's wait and fleet_wait_ready check `loading:` too.
PONG_S=$(python3 -c "print(f'{($T_PONG - $T0):.2f}')")
READY_S=$(python3 -c "print(f'{($T_READY - $T0):.2f}')")
python3 -c "import sys; sys.exit(0 if $T_READY - $T_PONG > 1.0 else 1)" || {
  echo "FAIL: ready came ${READY_S}s in and PONG ${PONG_S}s in — under a second apart, so"
  echo "      this run cannot distinguish 'answers' from 'serves' and its timing"
  echo "      assertions prove nothing. Lower CAP or seed more."
  exit 1
}
echo "  PONG at ${PONG_S}s, serving at ${READY_S}s"

echo "PASS: loading visible — a replica binds and answers PING/FLINTINFO from inside its initial full sync (role:loading, loading:1, loading_ms), refuses data commands and the FLINTNS pin with -LOADING, is not counted as a live replica while it seeds, and converges to role:replica with loading:0 (PONG ${PONG_S}s, serving ${READY_S}s)"
