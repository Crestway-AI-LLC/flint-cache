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
#
# THE LOAD IS SIZED, NOT GUESSED (2026-08-16). Arm B used to drive 32 threads
# of 4 KiB writes and hope the machine missed a 1 ms deadline. It failed 3 runs
# in 6 here, and the arithmetic says it always would have: the gate refuses
# when `inflight x service_us / 1000 > deadline_ms`, this box serves a 4 KiB
# write in ~18 us, and 32 x 18 = 576 us — an estimate of ZERO milliseconds, so
# NO positive deadline can ever be exceeded. The runs that did pass were riding
# a transient spike in the service-time EWMA, which is a coin toss, not a
# control.
#
# Worse than flaky: the failure it printed was "either the gate is not wired
# into admit_write_path, or this machine served 32 concurrent 4 KiB writes
# inside 1ms". The first branch accuses the product of shipping its write
# safety gate disconnected. A gate that cries that on a fast laptop teaches
# exactly one habit — re-run until green — and that habit is what would let the
# real version through.
#
# So the load is now sized to make the condition EXIST, and the drill checks
# that it does before it draws any conclusion from arm B.
#
# The size is bounded on BOTH sides and the drill enforces both, because the
# two failures look nothing alike and only one of them is a product bug:
#
#   too light -> arm B cannot fire, and used to accuse the gate of being
#                unwired. 64 x 16 KiB measured 1-13 ms across five runs, which
#                straddles the floor: that is the flake, one size up.
#   too heavy -> arm A sheds at the 2000 ms DEFAULT, which reads as "the
#                deadline fires on healthy traffic" when it really means the
#                load was never healthy traffic.
#
# 64 threads x 64 KiB measured 56-116 ms of predicted wait across three runs —
# 14-29x clear of the 4 ms floor arm B needs, 17x under the default arm A must
# not trip, and arm A shed 0 every time. WDL_THREADS/WDL_VSIZE move it for a
# machine at either extreme, and both guards name the knob when they fire.
set -u
THREADS="${WDL_THREADS:-64}"
PER="${WDL_PER:-40}"
VSIZE="${WDL_VSIZE:-65536}"
# The predicted wait must clear the smallest deadline the gate can act on by a
# real margin. 4 ms against a 1 ms deadline is 4x, and it is the floor at which
# this drill is entitled to conclude anything.
MIN_PREDICT_MS="${WDL_MIN_PREDICT_MS:-4}"
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
TAG, VSIZE = sys.argv[4], int(sys.argv[5])
VAL = os.urandom(VSIZE // 2).hex()    # a realistic write, not a no-op
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
echo "  load: $THREADS threads x $PER writes x $((VSIZE / 1024)) KiB"
A=$(python3 "$D/drive.py" 6392 "$THREADS" "$PER" arm-a "$VSIZE")
echo "  $A"
A_THR=$(echo "$A" | sed -n 's/.*throttled=\([0-9]*\).*/\1/p')
A_OTHER=$(echo "$A" | sed -n 's/.*other=\([0-9]*\).*/\1/p')
[ "$A_OTHER" = "0" ] || { echo "FAIL: unexpected replies under ordinary load"; exit 1; }
[ "$A_THR" = "0" ] || {
  echo "FAIL: the deadline shed $A_THR write(s) at the ${DEFAULT}ms default."
  echo "      If this load is ordinary traffic, that is a false positive on the"
  echo "      healthy path — worse than the unbounded hold it replaces, and the"
  echo "      fix is to raise the default or damp the estimator."
  echo "      But check the load first: $THREADS concurrent $((VSIZE / 1024)) KiB writes is a"
  echo "      DELIBERATELY heavy arm A, and on a slower box it can legitimately"
  echo "      exceed ${DEFAULT}ms. If that is what happened, lighten it with"
  echo "      WDL_THREADS/WDL_VSIZE — the product is not what changed."
  valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep -E '^write_(inflight|service_us|wait_est_ms):' | sed 's/^/        /'
  exit 1
}
SHED_A=$(valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep '^writes_shed_deadline:' | cut -d: -f2)
echo "  writes_shed_deadline: $SHED_A (must be 0)"
[ "${SHED_A:-1}" = "0" ] || { echo "FAIL: server counted a deadline shed under ordinary load"; exit 1; }

# IS THE POSITIVE CONTROL EVEN CONSTRUCTIBLE ON THIS MACHINE? Ask before
# running arm B, not after it comes back empty — the same two numbers the gate
# itself decides on, read from the node that just served arm A. A drill that
# cannot create the condition must say THAT, and must never offer "the gate is
# unwired" as an explanation for its own load being too light.
SERVICE_US=$(valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep '^write_service_us:' | cut -d: -f2)
PREDICT=$(( THREADS * ${SERVICE_US:-0} / 1000 ))
echo "  service ${SERVICE_US:-?}us/write, so ~${PREDICT}ms of wait at $THREADS concurrent"
[ "$PREDICT" -ge "$MIN_PREDICT_MS" ] || {
  echo "FAIL: the positive control cannot be built here. A write costs"
  echo "      ${SERVICE_US:-?}us on this box, so $THREADS concurrent writes are estimated to"
  echo "      wait ${PREDICT}ms — and the gate refuses only when that estimate EXCEEDS"
  echo "      the deadline, which no positive deadline below ${PREDICT}ms can do."
  echo "      This says nothing about whether the deadline works. Give the box"
  echo "      more to do: WDL_THREADS (now $THREADS) or WDL_VSIZE (now $VSIZE)."
  exit 1
}
[ "$PREDICT" -lt "$(( DEFAULT / 4 ))" ] || {
  echo "FAIL: ${PREDICT}ms of predicted wait is within 4x of the ${DEFAULT}ms default, so"
  echo "      arm A passing was luck rather than headroom. Lighten the load"
  echo "      (WDL_THREADS/WDL_VSIZE) — this drill must not run with its two"
  echo "      arms that close together."
  exit 1
}

echo "== arm B (positive control): the same load against a 1ms deadline"
valkey-cli -p 6392 FLINTCONFIG write-deadline-ms 1 >/dev/null
NOW=$(valkey-cli -p 6392 FLINTCONFIG | tr '\r' '\n' | grep '^write-deadline-ms:' | cut -d: -f2)
[ "$NOW" = "1" ] || { echo "FAIL: FLINTCONFIG did not move the deadline (got '$NOW')"; exit 1; }
echo "  deadline hot-set to ${NOW}ms without a restart"

BOUT=$(python3 "$D/drive.py" 6392 "$THREADS" "$PER" arm-b "$VSIZE")
echo "  $BOUT"
B_THR=$(echo "$BOUT" | sed -n 's/.*throttled=\([0-9]*\).*/\1/p')
B_OK=$(echo "$BOUT" | sed -n 's/^ok=\([0-9]*\).*/\1/p')
B_OTHER=$(echo "$BOUT" | sed -n 's/.*other=\([0-9]*\).*/\1/p')
B_PRESENT=$(echo "$BOUT" | sed -n 's/.*refused-but-present=\([0-9]*\).*/\1/p')
B_ABSENT=$(echo "$BOUT" | sed -n 's/.*accepted-but-absent=\([0-9]*\).*/\1/p')

[ "$B_OTHER" = "0" ] || { echo "FAIL: a refusal came back as something other than -THROTTLED"; exit 1; }
[ "${B_THR:-0}" -gt 0 ] || {
  # The constructibility check above already established that this load costs
  # ~${PREDICT}ms of estimated wait, which is what the gate reads, and the
  # deadline is 1ms. So the condition existed and nothing acted on it: the
  # gate is not reached on this path. That accusation is now earned rather
  # than offered as one of two guesses.
  echo "FAIL: nothing was refused at a 1ms deadline, on a load measured above at"
  echo "      ~${PREDICT}ms of estimated wait. The condition the gate exists for was"
  echo "      present and no write was refused, so admit_write_path is not"
  echo "      consulting the deadline. State at the end of the arm:"
  valkey-cli -p 6392 FLINTINFO | tr '\r' '\n' | grep -E '^write_(inflight|service_us|wait_est_ms|deadline_ms):' | sed 's/^/        /'
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
