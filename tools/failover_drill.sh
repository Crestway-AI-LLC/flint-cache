#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Manual failover drill: master + replica, load, kill -9 the master,
# FLINTPROMOTE the replica with a higher role epoch, verify it accepts
# writes with data intact — and that stale/equal epochs are FENCED.
# (The meta trio will automate the decision; the mechanics are these.)
set -euo pipefail
. "$(dirname "$0")/lib/fleet.sh"
# Declared so the SET of drills can be checked for port collisions —
# fleet_init only records the scope, it changes no behaviour here. A
# drill that declares nothing is invisible to assert_no_port_overlap,
# which is how failover and controller came to share 6440/6441 and
# reseed and lag_cap to share 6471/6472, unseen.
fleet_init $FLINT_DRILL_ROOT/flint-failover 6326 6327

KEYS="${1:-20000}"
MPORT="${2:-6326}"
RPORT="${3:-6327}"
# UNDER THE DECLARED SCOPE, not a shorthand of it (BUG-0047). fleet_init
# above declares `flint-failover`, and the harness decides which seats belong
# to this drill by that prefix OR by a declared port. `flint-fo-` matches
# neither, so every seat here was recognised by port alone and never by its
# directory — and the ZOMBIE, started outside fleet.sh's tracking, had nothing
# else identifying it. This drill passes serially and fails under
# FLINT_GATE_JOBS=3 with that zombie SIGKILLed; memory was measured at 12% of
# the box, so the OOM killer is ruled out.
#
# Whether this alone is the cause is what the next parallel run answers. It is
# correct regardless: a scope the harness is told about should be the scope
# actually used.
MDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-failover-m.XXXXXX)"
RDIR="$(mktemp -d $FLINT_DRILL_ROOT/flint-failover-r.XXXXXX)"
BIN="$(dirname "$0")/../target/release/flint-server"

cleanup() {
  pkill -f "flint-server --port $MPORT" 2>/dev/null || true
  pkill -f "flint-server --port $RPORT" 2>/dev/null || true
  rm -rf "$MDIR" "$RDIR"
}
trap cleanup EXIT

echo "== master :$MPORT, replica :$RPORT"
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.4
"$BIN" --port "$RPORT" --engine rocks --data-dir "$RDIR" --replica-of "127.0.0.1:$MPORT" &
fleet_wait_listen "$RPORT"
sleep 0.6

echo "== loading $KEYS keys"
awk -v n="$KEYS" 'BEGIN {
  for (i = 0; i < n; i++) {
    k = sprintf("key:%07d", i); v = sprintf("value-%07d", i)
    printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v
  }
}' | valkey-cli -p "$MPORT" --pipe | tail -1

# BUG-0088: say how close that load came to the write deadline, on a PASS as
# well as on a failure. The `errors:` line above reports only whether a write
# was REFUSED, and the server's own estimate is logged only once it has already
# been exceeded -- so every sample that exists is above the line by
# construction. Two gate runs failed here at 2017ms and 2033ms against a 2000ms
# deadline, and nothing in any green run could say whether a passing load sits
# at 200ms or at 1999ms. Those are very different bugs. One line turns every
# future gate run into a point on that distribution.
# `|| true` IS LOad-BEARING under `set -euo pipefail`.
#
# Without it this line killed the drill SILENTLY. On 2026-09-03 the master was
# refusing writes and had just lost its replication link; FLINTINFO returned
# non-zero, pipefail propagated it through the substitution, and `set -e` exited
# the script mid-run -- no FAIL line, no diagnosis, the log simply stopping after
# `errors: 2, replies: 20000`. A diagnostic line that can abort the run it is
# diagnosing is worse than no diagnostic at all, and it is the exact class of
# defect this drill was edited to remove.
INFO=$(valkey-cli -p "$MPORT" FLINTINFO 2>/dev/null | tr -d '\r' || true)
PEAK=$(printf '%s\n' "$INFO" | sed -n 's/^write_wait_peak_ms://p')
DLINE=$(printf '%s\n' "$INFO" | sed -n 's/^write_deadline_ms://p')
# The two TERMS at the moment of the peak, not just their product. est_ms is
# inflight x service_us, and they fail for opposite reasons -- more offered load
# than the seat can take, versus the seat itself having got slow. Four-way
# parallelism was measured to raise this peak 8.6x without touching memory
# (docs/bugs/0064), so which term moves is what names the resource.
PKI=$(printf '%s\n' "$INFO" | sed -n 's/^write_wait_peak_inflight://p')
PKS=$(printf '%s\n' "$INFO" | sed -n 's/^write_wait_peak_service_us://p')
# CAPABILITY ASSERT. An absent field must not read as a comfortable zero: that
# is the same "cannot look is not absent" this suite fails builds over, and a
# drill that silently stopped reporting the margin would look exactly like a
# drill reporting a wide one.
# THREE OUTCOMES, because two of them are not the same fault.
#
#   * the seat did not answer at all  -> transient, and this line is a
#     DIAGNOSTIC, not an assertion about the product. Say so and carry on; the
#     drill's real assertions are below and will catch anything that matters.
#     Failing here would mask them with a symptom of the same trouble.
#   * it answered but the fields are gone -> the build lost the instrument.
#     That IS a regression and must stop the run, or the margin silently
#     stops being reported and every later run reads as clean.
#   * it answered with the fields -> report them.
if [ -z "$INFO" ]; then
  echo "== write-wait peak UNREADABLE: the master did not answer FLINTINFO"
  echo "   (a diagnostic, not an assertion -- continuing to the real checks)"
elif [ -z "$PEAK" ] || [ -z "$DLINE" ]; then
  echo "FAIL: FLINTINFO answered but carries no write_wait_peak_ms/write_deadline_ms."
  echo "      The build has lost the instrument, which is not the same as the"
  echo "      margin being wide (docs/bugs/0088)."
  exit 1
else
  echo "== write-wait peak ${PEAK}ms of ${DLINE}ms deadline (inflight ${PKI:-UNREADABLE} x service ${PKS:-UNREADABLE}us; 0 = no write projected a measurable wait)"
fi

echo "== waiting for replica catch-up"
LAST="key:$(printf '%07d' $((KEYS - 1)))"
CAUGHT=0
for i in $(seq 1 100); do
  if [ "$(valkey-cli -p "$RPORT" GET "$LAST" 2>/dev/null || true)" = "value-$(printf '%07d' $((KEYS - 1)))" ]; then
    CAUGHT=1; break
  fi
  sleep 0.1
done
[ "$CAUGHT" = "1" ] || { echo "FAIL: replica never caught up"; valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' '; exit 1; }

echo "== stale-epoch promotion must be FENCED (current role epoch is (0,1))"
F1=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 1 2>&1 || true)
echo "$F1" | grep -q "FENCED" || { echo "FAIL: equal epoch not fenced: $F1"; exit 1; }
F2=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 0 2>&1 || true)
echo "$F2" | grep -q -E "FENCED|ERR" || { echo "FAIL: zero epoch accepted: $F2"; exit 1; }
echo "fencing OK: $F1"

echo "== kill -9 the master"
pkill -9 -f "flint-server --port $MPORT"
sleep 0.3

echo "== promote replica at role epoch (0,2)"
P=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 2)
echo "$P" | grep -q "OK promoted" || { echo "FAIL: promotion refused: $P"; exit 1; }
echo "$P"

echo "== promoted node accepts writes and kept the data"
W=$(valkey-cli -p "$RPORT" SET after-failover works)
[ "$W" = "OK" ] || { echo "FAIL: write after promotion: $W"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET after-failover)" = "works" ] || { echo "FAIL: read-back"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET key:0000000)" = "value-0000000" ] || { echo "FAIL: pre-failover data lost"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET "$LAST")" = "value-$(printf '%07d' $((KEYS - 1)))" ] || { echo "FAIL: tail data lost"; exit 1; }

echo "== role epoch is durable and visible"
valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' ' | grep -o "role:[a-z]* " | head -1
valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' ' | grep -o "role_epoch:[^ ]*" | head -1

echo "== re-promotion at the same epoch is FENCED (no double promotion)"
F3=$(valkey-cli -p "$RPORT" FLINTPROMOTE 0 2 2>&1 || true)
echo "$F3" | grep -q "FENCED" || { echo "FAIL: double promotion accepted: $F3"; exit 1; }

echo "== restart the promoted node WITH stale --replica-of: manifest must win"
pkill -f "flint-server --port $RPORT"
sleep 0.4
"$BIN" --port "$RPORT" --engine rocks --data-dir "$RDIR" --replica-of "127.0.0.1:$MPORT" &
fleet_wait_listen "$RPORT"
sleep 0.6
W2=$(valkey-cli -p "$RPORT" SET after-restart still-master)
[ "$W2" = "OK" ] || { echo "FAIL: promoted role lost after restart: $W2"; exit 1; }
[ "$(valkey-cli -p "$RPORT" GET after-failover)" = "works" ] || { echo "FAIL: post-promotion data lost"; exit 1; }

echo "== ZOMBIE: restart the OLD master on its old data dir"
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.6
# Hazard demonstrated: it still believes it is master (accepts a write).
Z=$(valkey-cli -p "$MPORT" SET zombie-write bad 2>&1)
[ "$Z" = "OK" ] || { echo "FAIL: expected the zombie hazard (write accepted), got: $Z"; exit 1; }
echo "zombie accepts writes (hazard confirmed; the trio's lease will close this window)"

echo "== fence the zombie with FLINTDEMOTE at a higher epoch"
CUR=$(valkey-cli -p "$RPORT" FLINTINFO | tr '\r' ' ' | grep -oE 'role_epoch:\([0-9]+,[0-9]+\)' | grep -oE '[0-9]+\)' | tr -d ')')
NEXT=$((CUR + 1))
D=$(valkey-cli -p "$MPORT" FLINTDEMOTE 0 "$NEXT")
echo "$D" | grep -q "OK demoted" || { echo "FAIL: demotion refused: $D"; exit 1; }
echo "$D"
RO=$(valkey-cli -p "$MPORT" SET should-fail x 2>&1 || true)
echo "$RO" | grep -q "READONLY" || { echo "FAIL: zombie still writable after demote: $RO"; exit 1; }

echo "== stale demotion epoch is FENCED"
F4=$(valkey-cli -p "$MPORT" FLINTDEMOTE 0 "$NEXT" 2>&1 || true)
echo "$F4" | grep -q "FENCED" || { echo "FAIL: equal-epoch demotion accepted: $F4"; exit 1; }

echo "== demotion survives restart (durable fencing)"
pkill -f "flint-server --port $MPORT"
sleep 0.4
"$BIN" --port "$MPORT" --engine rocks --data-dir "$MDIR" &
fleet_wait_listen "$MPORT"
sleep 0.6
RO2=$(valkey-cli -p "$MPORT" SET should-fail x 2>&1 || true)
echo "$RO2" | grep -q "READONLY" || { echo "FAIL: zombie writable again after restart: $RO2"; exit 1; }
echo "demoted role held across restart"

echo "PASS: epoch-fenced promotion + demotion, durable roles, zombie fenced, data intact"
