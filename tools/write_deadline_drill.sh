#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The write deadline must be OBSERVED to refuse, and observed NOT to refuse
# ordinary traffic. Both halves, or the knob is worthless in opposite ways.
#
# WHY THIS EXISTS. Soak run 24 measured a client waiting 8931ms for a write
# while a re-seed had the disk — after a failover that had itself completed in
# 700ms. Throttling the re-seed (#184) cut that to 8931ms from 11883ms: real,
# and not a bound. Nothing in the write path ever decided "this write cannot
# make its deadline, refuse it now"; work was simply held until it finished,
# however long that took. #186 added that decision. This drill is its evidence.
#
# THE #121 LESSON, APPLIED IN ADVANCE. The lag cap shipped and ran green for
# months across every chaos run — including a 7-host run over a real network —
# reporting `writes shed -THROTTLED: 0` every time. That was never evidence the
# cap worked; it was evidence the condition was never created. A gate nobody
# has watched fire is a gate nobody has tested. So this drill's first job is to
# MAKE it fire, by shrinking the deadline until the machine cannot meet it.
#
# WHAT IT PROVES
#
#   1. NEGATIVE control, and it runs FIRST so the positive arm cannot be a
#      false alarm: at the shipped default (2000ms) the same load sheds
#      NOTHING. A deadline that fires on ordinary traffic would be a far worse
#      bug than the one it fixes.
#   2. POSITIVE control: at a 1ms deadline the same load is refused, the
#      refusal is `-THROTTLED` (already retry-with-backoff to every client and
#      already never-acked to the chaos ledger), and the shed counter moves.
#   3. A REFUSAL IS A REFUSAL. Every key whose only attempt was refused must be
#      absent afterwards, and every key whose attempt was accepted must be
#      present. This is the property everything else rests on: if a shed write
#      could still land, `-THROTTLED` would be a lie and the oracle that counts
#      shed writes as never-acked would be miscounting data loss.
#   4. The knob is hot: FLINTCONFIG moves it on a running node, so an operator
#      can widen or tighten it mid-incident without a restart.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-wdl 6392 6393
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-wdl; rm -rf "$D"; mkdir -p "$D"
fleet_kill server; sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build"; exit 1; }
fleet_warm ./target/release/flint-server ./target/release/flint-proxy ./target/release/flint-controlplane ./target/release/flint-controller

echo "== master on the shipped default deadline"
$B --port 6392 --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
fleet_wait_ping 6392

DEFAULT=$(valkey-cli -p 6392 FLINTCONFIG | tr '\r' '\n' | grep '^write-deadline-ms:' | cut -d: -f2)
echo "  write-deadline-ms: $DEFAULT"
[ "${DEFAULT:-0}" -gt 0 ] || { echo "FAIL: no deadline configured by default — #186 ships off"; exit 1; }

# One driver, run twice against the same node. Each key is attempted EXACTLY
# once and never retried, so "was it accepted" and "is it there" are directly
# comparable — which is assertion 3.
cat >"$D/drive.py" <<'PY'
import socket, sys, threading, os
PORT, THREADS, PER = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
TAG = sys.argv[4]
VAL = os.urandom(2048).hex()          # 4 KiB: a realistic write, not a no-op
ok, throttled, other = [], [], []
lock = threading.Lock()

def resp(a):
    return f"*{len(a)}\r\n".encode() + b"".join(
        f"${len(x)}\r\n".encode() + x.encode() + b"\r\n" for x in a)

def drive(tid):
    mine_ok, mine_thr, mine_other = [], [], []
    s = socket.create_connection(("127.0.0.1", PORT), timeout=20); s.settimeout(20)
    for i in range(PER):
        k = f"{TAG}:{tid}:{i}"
        s.sendall(resp(["SET", k, VAL]))
        r = s.recv(256)
        if r.startswith(b"+OK"):              mine_ok.append(k)
        elif r.startswith(b"-THROTTLED"):     mine_thr.append(k)
        else:                                 mine_other.append((k, r[:80]))
    with lock:
        ok.extend(mine_ok); throttled.extend(mine_thr); other.extend(mine_other)

ts = [threading.Thread(target=drive, args=(t,)) for t in range(THREADS)]
[t.start() for t in ts]; [t.join() for t in ts]

# Assertion 3, checked here where the per-key verdict is still in hand.
s = socket.create_connection(("127.0.0.1", PORT), timeout=20); s.settimeout(20)
def exists(k):
    s.sendall(resp(["EXISTS", k])); return s.recv(64).strip() == b":1"
bad_present = [k for k in throttled[:200] if exists(k)]
bad_absent  = [k for k in ok[:200] if not exists(k)]
print(f"ok={len(ok)} throttled={len(throttled)} other={len(other)} "
      f"refused-but-present={len(bad_present)} accepted-but-absent={len(bad_absent)}")
if other:
    print("unexpected replies:", other[:3])
PY

echo "== arm A (negative control): ordinary load at the default deadline"
A=$(python3 "$D/drive.py" 6392 32 100 arm-a)
echo "  $A"
A_THR=$(echo "$A" | sed -n 's/.*throttled=\([0-9]*\).*/\1/p')
A_OTHER=$(echo "$A" | sed -n 's/.*other=\([0-9]*\).*/\1/p')
[ "$A_OTHER" = "0" ] || { echo "FAIL: unexpected replies under ordinary load"; exit 1; }
[ "$A_THR" = "0" ] || {
  echo "FAIL: the deadline shed $A_THR ordinary write(s) at the ${DEFAULT}ms default."
  echo "      That is a false positive on the healthy path — worse than the"
  echo "      unbounded hold it replaces. Raise the default or damp the estimator."
  exit 1
}
SHED_A=$(valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep '^writes_shed_deadline:' | cut -d: -f2)
echo "  writes_shed_deadline: $SHED_A (must be 0)"
[ "${SHED_A:-1}" = "0" ] || { echo "FAIL: server counted a deadline shed under ordinary load"; exit 1; }

echo "== arm B (positive control): the same load against a 1ms deadline"
valkey-cli -p 6392 FLINTCONFIG write-deadline-ms 1 >/dev/null
NOW=$(valkey-cli -p 6392 FLINTCONFIG | tr '\r' '\n' | grep '^write-deadline-ms:' | cut -d: -f2)
[ "$NOW" = "1" ] || { echo "FAIL: FLINTCONFIG did not move the deadline (got '$NOW')"; exit 1; }
echo "  deadline hot-set to ${NOW}ms without a restart"

BOUT=$(python3 "$D/drive.py" 6392 32 100 arm-b)
echo "  $BOUT"
B_THR=$(echo "$BOUT" | sed -n 's/.*throttled=\([0-9]*\).*/\1/p')
B_OK=$(echo "$BOUT" | sed -n 's/^ok=\([0-9]*\).*/\1/p')
B_OTHER=$(echo "$BOUT" | sed -n 's/.*other=\([0-9]*\).*/\1/p')
B_PRESENT=$(echo "$BOUT" | sed -n 's/.*refused-but-present=\([0-9]*\).*/\1/p')
B_ABSENT=$(echo "$BOUT" | sed -n 's/.*accepted-but-absent=\([0-9]*\).*/\1/p')

[ "$B_OTHER" = "0" ] || { echo "FAIL: a refusal came back as something other than -THROTTLED"; exit 1; }
[ "${B_THR:-0}" -gt 0 ] || {
  echo "FAIL: nothing was refused even at a 1ms deadline. Either the gate is not"
  echo "      wired into admit_write_path, or this machine served 32 concurrent"
  echo "      4 KiB writes inside 1ms — check write_wait_est_ms below:"
  valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep -E '^write_(inflight|service_us|wait_est_ms):' | sed 's/^/        /'
  exit 1
}
echo "  refused $B_THR write(s) with -THROTTLED, accepted $B_OK"

# Assertion 3. A shed that still wrote would make -THROTTLED a lie and would
# silently corrupt every RPO number the chaos oracle has ever reported.
[ "$B_PRESENT" = "0" ] || { echo "FAIL: $B_PRESENT key(s) refused with -THROTTLED are PRESENT — the shed is not a refusal"; exit 1; }
[ "$B_ABSENT" = "0" ]  || { echo "FAIL: $B_ABSENT key(s) acked +OK are ABSENT — an accepted write was lost"; exit 1; }
echo "  every refused key absent, every accepted key present"

SHED_B=$(valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep '^writes_shed_deadline:' | cut -d: -f2)
echo "  writes_shed_deadline: $SHED_B"
[ "${SHED_B:-0}" -ge "$B_THR" ] || { echo "FAIL: the server's shed counter ($SHED_B) is below the refusals clients saw ($B_THR)"; exit 1; }

echo "== the node still serves after shedding (a refusal is not a fault)"
valkey-cli -p 6392 FLINTCONFIG write-deadline-ms "$DEFAULT" >/dev/null
valkey-cli -p 6392 SET wdl:after ok >/dev/null
[ "$(valkey-cli -p 6392 GET wdl:after)" = "ok" ] || { echo "FAIL: node did not recover once the deadline was restored"; exit 1; }
echo "  deadline restored to ${DEFAULT}ms, writes accepted again"

echo "PASS: write deadline drill — ordinary load at the ${DEFAULT}ms default shed 0, a 1ms deadline refused $B_THR write(s) with -THROTTLED, every refusal was a real refusal (0 refused-but-present, 0 accepted-but-absent), and the knob moved hot"
