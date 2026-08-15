#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The full-sync RATE cap (--fullsync-rate-bytes) must be observed to slow a
# re-seed. Not to be confused with tools/fullsync_cap_drill.sh, which bounds
# how MANY full-syncs run at once (--max-fullsync); this one bounds how FAST
# one of them is allowed to go.
#
# WHY. Soak run 23 measured a client waiting 11883ms for a write after a
# failover that had itself completed in 700ms. The gap was not the promotion:
# the restarted seat was pulling a 104-file checkpoint off the newly promoted
# master as fast as the disk allowed, and the write path got what was left.
# One mechanism explains #177 (43.6s), #181 (11.5s) and run 23. #184 added
# this cap; run 24 then measured 8931ms, 25% better on the same onset — which
# is exactly why the cap is a mitigation and #186's deadline is the guarantee.
#
# HOW IT ASSERTS — a RATIO, never a wall clock. A drill that asserted "the
# capped sync took more than N seconds" would encode this laptop's disk into
# the gate and fail on a slower CI box for a reason that has nothing to do
# with the cap. So both arms run here, back to back, on the same dataset and
# the same machine, and the assertion is that the capped arm is materially
# slower than the uncapped one.
#
# THE POSITIVE CONTROL RUNS FIRST, and it is the half that makes the result
# mean anything. tools/fullsync_cap_drill.sh is out of the gate today because
# on a fast host its herd never overlaps, so it asserts a condition it failed
# to create — the #121 failure shape. The guard against that here is explicit:
# the UNCAPPED arm must beat the cap by a clear margin, or this machine could
# not have exceeded the cap anyway and the capped arm proves nothing. If that
# check trips, the drill says so and tells you to lower the cap, rather than
# reporting a pass it did not earn.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-fsrate 6394 6395 6397
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-fsrate; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

CAP=$((4 * 1024 * 1024))          # 4 MiB/s: slow enough to be unmistakable
MARGIN=2                          # uncapped must beat the cap by this factor

echo "== master, uncapped, holding a dataset worth streaming"
$B --port 6394 --engine rocks --data-dir "$D/m" --fullsync-rate-bytes 0 2>"$D/m.log" &
fleet_wait_ping 6394

# 32 MB of INCOMPRESSIBLE values. Hex or repeated text would compress in the
# block cache and on disk, so the bytes actually crossing the wire would be a
# fraction of what the drill thinks it seeded — and the measured rate would
# be wrong in the direction that hides a broken cap (#169, same mistake).
python3 - <<PY
import socket, os
def resp(a):
    return b"*%d\r\n" % len(a) + b"".join(b"\$%d\r\n%s\r\n" % (len(x), x) for x in a)
s = socket.create_connection(("127.0.0.1", 6394), timeout=30); s.settimeout(30)
for i in range(4000):
    s.sendall(resp([b"SET", b"fsr:%d" % i, os.urandom(8192)])); s.recv(64)
s.sendall(resp([b"FLINTSNAPSHOT", b"$D/snap"])); s.recv(256)   # flush memtables to SSTs
PY
SST=$(valkey-cli -p 6394 FLINTINFO | tr '\r' '\n' | grep '^sst_bytes:' | cut -d: -f2)
echo "  master holds $SST bytes of SSTs"
[ "${SST:-0}" -gt $((16 * 1024 * 1024)) ] || { echo "FAIL: dataset too small to time a transfer ($SST bytes)"; exit 1; }

# A replica binds its listener only after its initial full sync completes, so
# "spawn to first PONG" is the sync itself plus a fixed process/rocks-open
# cost. That cost is identical in both arms and small against a 32MB transfer,
# which is the other reason the assertion is a ratio.
time_reseed() {   # $1 = replica port, $2 = dir suffix
  local t0 t1
  t0=$(python3 -c 'import time; print(time.time())')
  # BOTH streams to the log, not just stderr. This function's output is read
  # through `$(...)`, and a backgrounded child that inherits that pipe holds
  # it open — so leaving stdout attached makes the command substitution block
  # until the server exits, which is forever. Cost the first run of this drill
  # nine minutes of looking like a hung full sync when the sync had finished
  # in under a second.
  $B --port "$1" --engine rocks --data-dir "$D/$2" --replica-of 127.0.0.1:6394 >"$D/$2.log" 2>&1 &
  fleet_wait_ping "$1" >/dev/null 2>&1
  t1=$(python3 -c 'import time; print(time.time())')
  python3 -c "print(f'{($t1 - $t0):.3f}')"
}

echo "== arm A (positive control): uncapped re-seed"
UNCAPPED=$(time_reseed 6395 r1)
UNCAPPED_RATE=$(python3 -c "print(int($SST / max($UNCAPPED, 0.001)))")
echo "  uncapped: ${UNCAPPED}s  (~$((UNCAPPED_RATE / 1024 / 1024)) MiB/s)"
[ "$UNCAPPED_RATE" -gt $((CAP * MARGIN)) ] || {
  echo "FAIL: this host re-seeds at only $((UNCAPPED_RATE / 1024 / 1024)) MiB/s uncapped, which is"
  echo "      not ${MARGIN}x the ${CAP} B/s cap under test. The capped arm would then be"
  echo "      slow for the machine's reasons, not the cap's, and a pass would"
  echo "      mean nothing — the same hole that keeps fullsync_cap out of the"
  echo "      gate. Lower CAP in this drill rather than loosening the assert."
  exit 1
}

echo "== arm B: the same re-seed with the rate capped at $((CAP / 1024 / 1024)) MiB/s"
valkey-cli -p 6394 FLINTCONFIG fullsync-rate-bytes "$CAP" >/dev/null
NOW=$(valkey-cli -p 6394 FLINTCONFIG | tr '\r' '\n' | grep '^fullsync-rate-bytes:' | cut -d: -f2)
[ "$NOW" = "$CAP" ] || { echo "FAIL: FLINTCONFIG did not move the cap (got '$NOW')"; exit 1; }
CAPPED=$(time_reseed 6397 r2)
CAPPED_RATE=$(python3 -c "print(int($SST / max($CAPPED, 0.001)))")
echo "  capped:   ${CAPPED}s  (~$((CAPPED_RATE / 1024 / 1024)) MiB/s)"

# The cap is an AVERAGE-rate limiter (migrate::Pacer), so a burst may briefly
# exceed it; what must hold is the average over a 32MB transfer. 1.5x leaves
# room for the fixed process/open cost being counted as transfer time, which
# biases the measured rate UP and so can only make this assert stricter.
[ "$CAPPED_RATE" -lt $((CAP * 3 / 2)) ] || {
  echo "FAIL: capped re-seed averaged $CAPPED_RATE B/s against a $CAP B/s cap."
  echo "      The pacer is not throttling the full-sync send loop."
  exit 1
}
RATIO=$(python3 -c "print(f'{($CAPPED / max($UNCAPPED, 0.001)):.1f}')")
echo "  capped/uncapped: ${RATIO}x slower"
python3 -c "import sys; sys.exit(0 if $CAPPED > $UNCAPPED * $MARGIN else 1)" || {
  echo "FAIL: the cap did not materially slow the transfer (${RATIO}x, wanted >${MARGIN}x)"
  exit 1
}

echo "== both replicas seeded correctly — throttling must not truncate"
for P in 6395 6397; do
  [ "$(valkey-cli -p $P GET fsr:2000 | wc -c | tr -d ' ')" = "$(valkey-cli -p 6394 GET fsr:2000 | wc -c | tr -d ' ')" ] \
    || { echo "FAIL: replica on $P did not converge to the master's data"; exit 1; }
done
echo "  both replicas serve the master's data"

echo "PASS: full-sync rate drill — uncapped $((UNCAPPED_RATE / 1024 / 1024)) MiB/s vs capped $((CAPPED_RATE / 1024 / 1024)) MiB/s against a $((CAP / 1024 / 1024)) MiB/s cap (${RATIO}x slower), cap moved hot, both replicas converged"
