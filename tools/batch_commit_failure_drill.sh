# SPDX-License-Identifier: Elastic-2.0
# A batch whose commit FAILS must answer every held reply with an error, and
# must have written nothing (ADR-0027, write-path roadmap item 7).
#
# ADR-0027 defers a run of pure writes and commits them as one engine
# WriteBatch. Because nothing is flushed to the client until the commit
# returns, a failed commit is still expressible: the held `+OK`s are rewritten
# as errors. That is the whole reason the replies are held rather than
# streamed -- and it had never been executed, because only `apply_writes` can
# fail and forcing a real RocksDB write error needs a full disk or a closed
# handle, neither of which a drill can create without testing something else.
# FLINT_BATCH_COMMIT_FAIL arms it.
#
# TWO ARMS, because "the batch failed correctly" means nothing unless the same
# pipeline succeeds correctly without the seam:
#
#   A (negative control): no seam. Every reply +OK, every key present.
#   B (positive control): seam set. Every reply an error, NO key present, and
#     the connection still serves afterwards -- a failed batch must not wedge
#     the connection or leak the stripe guards it was holding.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-bcf 6414 6415
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-bcf; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }

# A pipeline of PURE SETs on one connection: that is what batch_eligible
# accepts, and a batch of one would prove nothing about rewriting N replies.
cat >"$D/pipe.py" <<'PY'
import socket, sys
port, tag, n = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
s = socket.create_connection(("127.0.0.1", port), timeout=20); s.settimeout(20)
buf = b"".join(
    b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$3\r\nval\r\n" % (len(k), k)
    for k in [("%s:%d" % (tag, i)).encode() for i in range(n)])
s.sendall(buf)
data = b""
while data.count(b"\r\n") < n:
    d = s.recv(65536)
    if not d: break
    data += d
ok = data.count(b"+OK\r\n")
err = data.count(b"-")
# The connection must still be usable after the batch, failed or not.
s.sendall(b"*1\r\n$4\r\nPING\r\n")
alive = b"PONG" in s.recv(64)
print("ok=%d err=%d alive=%s" % (ok, err, "yes" if alive else "no"))
PY

N=64
echo "== arm A (negative control): the same pipeline with NO seam"
$B --port 6414 --engine rocks --data-dir "$D/a" 2>"$D/a.log" &
fleet_wait_listen 6414; fleet_wait_ping 6414
A=$(python3 "$D/pipe.py" 6414 arma $N) || { echo "FAIL: arm A client"; exit 1; }
echo "  $A"
case "$A" in
  "ok=$N err=0 alive=yes") ;;
  *) echo "FAIL: an unseamed pipeline of $N pure SETs must be $N x +OK, got: $A"; exit 1 ;;
esac
PRESENT_A=$(valkey-cli -p 6414 EXISTS arma:0 arma:63 | tr -d '\r')
[ "$PRESENT_A" = "2" ] || { echo "FAIL: arm A wrote nothing (EXISTS returned $PRESENT_A of 2)"; exit 1; }
echo "  keys present, connection alive"
fleet_kill server; sleep 0.3

echo "== arm B (positive control): FLINT_BATCH_COMMIT_FAIL=1"
FLINT_BATCH_COMMIT_FAIL=1 $B --port 6415 --engine rocks --data-dir "$D/b" 2>"$D/b.log" &
fleet_wait_listen 6415; fleet_wait_ping 6415
BOUT=$(python3 "$D/pipe.py" 6415 armb $N) || { echo "FAIL: arm B client"; exit 1; }
echo "  $BOUT"
B_OK=$(echo "$BOUT" | sed -n 's/^ok=\([0-9]*\).*/\1/p')
B_ERR=$(echo "$BOUT" | sed -n 's/.*err=\([0-9]*\).*/\1/p')
B_ALIVE=$(echo "$BOUT" | sed -n 's/.*alive=\(.*\)$/\1/p')

[ "${B_ERR:-0}" -ge "$N" ] || {
  echo "FAIL: the positive control COULD NOT BE ARMED — a failing commit"
  echo "      returned only ${B_ERR:-0} error(s) for $N held replies. Either the"
  echo "      seam no longer reaches commit_ops, or the writes did not batch"
  echo "      at all (a batch of one cannot show this). Arm A proves nothing"
  echo "      until this arm can fail."
  exit 1
}
[ "${B_OK:-1}" = "0" ] || {
  echo "FAIL: $B_OK reply/replies said +OK on a batch that never committed."
  echo "      An ack for a write that was discarded is the one outcome this"
  echo "      path exists to prevent."
  exit 1
}
echo "  every held reply became an error, none acked"

# NOTHING may be present: the batch is staged in memory and commit_ops is what
# writes it, so a failure must leave the store untouched.
LEAKED=$(valkey-cli -p 6415 EXISTS armb:0 armb:1 armb:63 | tr -d '\r')
[ "$LEAKED" = "0" ] || {
  echo "FAIL: $LEAKED of 3 sampled keys are PRESENT after a failed commit —"
  echo "      the batch wrote despite reporting failure, which makes the error"
  echo "      a lie and every RPO number downstream of it wrong."
  exit 1
}
echo "  no key from the failed batch is present"

[ "$B_ALIVE" = "yes" ] || {
  echo "FAIL: the connection did not answer PING after the failed batch — a"
  echo "      failed commit must not wedge it or leak the stripe guards it held"
  exit 1
}
# AND NO GUARD LEAKED, which is a different question from "did it error".
#
# The seam is process-wide, so a later write cannot be expected to SUCCEED --
# it commits through the same path and fails the same way. What distinguishes
# a released guard from a leaked one is not the reply, it is whether there is
# a reply at all: a leaked global write guard blocks the NEXT writer, so the
# symptom would be a hang, not an error. So: bound the wait and require a
# prompt answer.
#
# The first version of this drill asserted the later write returned +OK, and
# failed against a perfectly healthy server. Worth keeping in mind: the check
# was wrong in a way that looked exactly like the defect it was hunting.
( valkey-cli -p 6415 SET after:the:failure ok > "$D/after.out" 2>&1 ) &
AFTER=$!
for _ in $(seq 1 50); do kill -0 $AFTER 2>/dev/null || break; sleep 0.2; done
if kill -0 $AFTER 2>/dev/null; then
  kill -9 $AFTER 2>/dev/null
  echo "FAIL: a write issued after the failed batch never answered (10s)."
  echo "      commit_pending drops its stripe and global guards after"
  echo "      commit_ops on BOTH paths; a hang here means the failure path"
  echo "      stopped doing that, and the next writer inherits the deadlock."
  exit 1
fi
AFTER_REPLY=$(tr -d '\r' < "$D/after.out")
case "$AFTER_REPLY" in
  *FLINT_BATCH_COMMIT_FAIL*|*"commit failed"*)
    echo "  a later write answers promptly (with the injected error): no guard leaked" ;;
  OK) echo "FAIL: a write committed while FLINT_BATCH_COMMIT_FAIL was armed — the seam is not reaching commit_ops"; exit 1 ;;
  *)  echo "FAIL: a later write answered '$AFTER_REPLY', which is neither the injected error nor OK"; exit 1 ;;
esac

echo "PASS: batch commit failure drill — $N held replies all became errors with 0 acks and 0 keys written, the connection survived with no guard leaked, and the same pipeline without the seam wrote all $N, so the check is known to be able to fail"
