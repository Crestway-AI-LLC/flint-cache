# SPDX-License-Identifier: Elastic-2.0
# A pipelined client must not pay a delayed ACK per round trip (BUG-0078).
#
# The node reads 16 KiB at a time and answers each read. Without TCP_NODELAY
# on the accepted socket, a reply written while the client's own send is still
# in flight is held by Nagle until the peer's delayed-ACK timer fires, so every
# round trip costs ~50ms. It is invisible below the threshold and catastrophic
# above it: at 1 KiB values, depth 12 measured 137,597 writes/s and depth 16
# measured 323. Nothing errors, nothing is logged, and no counter moves.
#
# TWO ARMS, because an assertion that something is FAST proves nothing unless
# the same measurement can be made to fail:
#
#   A (negative control): the shipped server. Depth 32 must round-trip well
#     inside the delayed-ACK timer.
#   B (positive control): the same server with FLINT_NAGLE_TEST=1, which skips
#     set_nodelay. The stall MUST reappear. If it does not, this drill cannot
#     see the defect it exists to catch and says so rather than passing.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-nodelay 6407 6408
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-nodelay; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

cat >"$D/rt.py" <<'PY'
import os, socket, sys, time
# Reports MILLISECONDS PER ROUND TRIP, which is the quantity the defect moves.
# Throughput would confound it with how much work each round trip carries.
PORT, DEPTH, ROUNDS = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
VS = 1024
val = os.urandom(VS // 2).hex()
s = socket.create_connection(("127.0.0.1", PORT), timeout=30)
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)   # client side is never the question
hdr = b"*3\r\n$3\r\nSET\r\n"
n = 0
# One untimed round trip first: the very first exchange on a fresh connection
# carries slow-start and the RESP dialect handshake, and folding that into a
# per-round-trip mean would flatter or damn the arm depending on ROUNDS.
for warm in (True, False):
    if not warm:
        t0 = time.time()
    for _ in range(ROUNDS if not warm else 1):
        buf = bytearray()
        for _ in range(DEPTH):
            k = ("rt:%d" % n).encode(); n += 1
            buf += hdr + b"$%d\r\n" % len(k) + k + b"\r\n" + b"$%d\r\n" % VS + val.encode() + b"\r\n"
        s.sendall(buf)
        got = 0
        while got < DEPTH:
            d = s.recv(262144)
            if not d:
                sys.exit("closed")
            got += d.count(b"\r\n")
print("%.1f" % (1000.0 * (time.time() - t0) / ROUNDS))
PY

# 32 x 1 KiB is ~33 KiB, comfortably past the 16 KiB read the node answers at.
DEPTH=32; ROUNDS=20
# The delayed-ACK timer is tens of ms; a healthy round trip here is under one.
# 10ms is therefore a wide margin in the only direction that matters, chosen so
# a loaded CI box cannot fail this by being slow.
BUDGET_MS=10
ARMED_MS=20

echo "== arm A (negative control): the shipped server"
$B --port 6407 --engine rocks --data-dir "$D/a" 2>"$D/a.log" &
fleet_wait_listen 6407
sleep 0.5
A_MS=$(python3 "$D/rt.py" 6407 $DEPTH $ROUNDS) || { echo "FAIL: arm A client"; exit 1; }
echo "  depth $DEPTH x 1 KiB: ${A_MS}ms per round trip"
fleet_kill server; sleep 0.3

echo "== arm B (positive control): the same server, FLINT_NAGLE_TEST=1"
FLINT_NAGLE_TEST=1 $B --port 6408 --engine rocks --data-dir "$D/b" 2>"$D/b.log" &
fleet_wait_listen 6408
sleep 0.5
B_MS=$(python3 "$D/rt.py" 6408 $DEPTH $ROUNDS) || { echo "FAIL: arm B client"; exit 1; }
echo "  depth $DEPTH x 1 KiB, Nagle left on: ${B_MS}ms per round trip"
fleet_kill server; sleep 0.3

awk -v b="$B_MS" -v armed="$ARMED_MS" 'BEGIN { exit !(b > armed) }' || {
  echo "FAIL: the positive control COULD NOT BE ARMED — with TCP_NODELAY"
  echo "      skipped, a ${DEPTH}-deep pipeline still round-tripped in ${B_MS}ms,"
  echo "      under the ${ARMED_MS}ms that says the stall is present. This is a"
  echo "      failure to CREATE the condition, not evidence the node is well:"
  echo "      this kernel may coalesce differently, or the seam may have"
  echo "      stopped reaching the accept path. Arm A's ${A_MS}ms proves"
  echo "      nothing until this arm can fail."
  exit 1
}
echo "  control armed: leaving Nagle on costs ${B_MS}ms a round trip"

awk -v a="$A_MS" -v budget="$BUDGET_MS" 'BEGIN { exit !(a < budget) }' || {
  echo "FAIL: a ${DEPTH}-deep pipeline round-tripped in ${A_MS}ms, over the"
  echo "      ${BUDGET_MS}ms budget, against ${B_MS}ms with Nagle deliberately left"
  echo "      on. That is the BUG-0078 signature: the accepted socket is not"
  echo "      getting TCP_NODELAY, and every pipelined client pays a delayed"
  echo "      ACK per round trip while nothing errors and no counter moves."
  exit 1
}

echo "PASS: pipeline nodelay drill — a ${DEPTH}x1KiB pipeline round-trips in ${A_MS}ms against a ${BUDGET_MS}ms budget, and the same server with TCP_NODELAY skipped takes ${B_MS}ms, so the check is known to be able to fail"
