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

# WAL fsync counter BEFORE the load, to pair with the cadence and the service
# time after it. `|| true` because this drill is set -euo pipefail and a
# diagnostic must never abort the run it is diagnosing.
FS0=$(valkey-cli -p "$MPORT" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^wal_fsync_total://p' || true)

echo "== loading $KEYS keys"
# THROUGH fleet_load_resp. Piping straight into `--pipe` under this drill's
# `set -euo pipefail` made a single -THROTTLED abort the run: --pipe exits
# non-zero when it counts ANY error. Two gate runs died here on 2026-09-04 with
# 1 and 2 errors of 20,000 --
#
#   THROTTLED write would wait ~2002ms (inflight 244 x service 8206us),
#     past --write-deadline-ms 2000, retry with backoff
#
# -- which is the write deadline reacting to a slow runner (244 x 8.206ms is
# 2002ms, the estimator sitting exactly on its own line), not a failover fault.
# A shed write is REFUSED, never acked, so it cannot affect what this drill
# asserts about the keys that were.
#
# The ceiling is 0.05% of the load, the same rule restart_drill uses.
LOAD_GEN=$(mktemp "${FLINT_DRILL_ROOT:-/tmp}/flint-fo-load.XXXXXX")
awk -v n="$KEYS" 'BEGIN {
  for (i = 0; i < n; i++) {
    k = sprintf("key:%07d", i); v = sprintf("value-%07d", i)
    printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v
  }
}' > "$LOAD_GEN"
fleet_load_resp "$MPORT" "cat $LOAD_GEN" "$KEYS" "$(( KEYS / 2000 + 1 ))" || exit 1
rm -f "$LOAD_GEN"

# BUG-0088: say how close that load came to the write deadline, on a PASS as
# well as on a failure. The `errors:` line above reports only whether a write
# was REFUSED, and the server's own estimate is logged only once it has already
# been exceeded -- so every sample that exists is above the line by
# construction. Two gate runs failed here at 2017ms and 2033ms against a 2000ms
# deadline, and nothing in any green run could say whether a passing load sits
# at 200ms or at 1999ms. Those are very different bugs. One line turns every
# future gate run into a point on that distribution.
# `|| true` IS LOAD-BEARING under `set -euo pipefail`.
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
  # CADENCE AND COUNT TOGETHER, because neither alone can be read.
  #
  # The fsync here is PERIODIC, not per-write: WAL_FSYNC_MS is a timer, so a
  # write only pays for one if it lands on a tick. That makes a per-write
  # service inflation an unlikely thing for fsync to cause directly, and it
  # makes a zero COUNT ambiguous -- a fast load can span no tick at all and
  # report 0 while the mechanism is perfectly live.
  #
  # Recorded because two wrong readings were nearly shipped from this spot in
  # one sitting: first a count alone, called structurally zero on the belief
  # that the cadence defaults to 0 (the static does; the running seat reports
  # 500ms); then a cadence alone, worded as though a non-zero cadence put fsync
  # inside every write. Both numbers, and the caveat, or neither.
  # ROCKSDB'S OWN BACKPRESSURE, which separates two of the remaining candidates
  # at no cost -- these fields are already in FLINTINFO and INFO is already read.
  #
  # Per-write service time inflates ~10x under four-way parallelism while queue
  # depth stays flat (docs/bugs/0064). If that is COMPACTION DEBT, the engine
  # says so itself: write_stopped, a non-zero delayed_write_rate, growing
  # l0_files or pending_compaction_bytes are RocksDB throttling the write path
  # deliberately. If instead all of these are quiet while service time is still
  # 10x, the engine believes it is healthy and the time is going somewhere it
  # cannot see -- the OS descheduling it, or contention below the filesystem.
  #
  # Instantaneous at INFO time rather than at the peak, so a quiet reading here
  # is weaker evidence than a loud one: debt can drain between the peak and this
  # read. A loud reading is conclusive, a quiet one is suggestive.
  WST=$(printf '%s\n' "$INFO" | sed -n 's/^write_stopped://p')
  DWR=$(printf '%s\n' "$INFO" | sed -n 's/^delayed_write_rate://p')
  L0F=$(printf '%s\n' "$INFO" | sed -n 's/^l0_files://p')
  PCB=$(printf '%s\n' "$INFO" | sed -n 's/^pending_compaction_bytes://p')
  echo "== engine backpressure at read: write_stopped=${WST:-?} delayed_write_rate=${DWR:-?} l0_files=${L0F:-?} pending_compaction_bytes=${PCB:-?}"
  FSMS=$(printf '%s\n' "$INFO" | sed -n 's/^wal_fsync_ms://p')
  FS1=$(printf '%s\n' "$INFO" | sed -n 's/^wal_fsync_total://p')
  if [ -n "${FS0:-}" ] && [ -n "${FS1:-}" ]; then
    echo "== wal fsync: cadence ${FSMS:-UNREADABLE}ms (periodic, not per-write), $((FS1 - FS0)) fsync(s) during the load of $KEYS writes"
  else
    echo "== wal fsync: cadence ${FSMS:-UNREADABLE}ms, count UNREADABLE (pre=${FS0:-?} post=${FS1:-?})"
  fi
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
