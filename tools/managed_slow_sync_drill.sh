#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A managed controller must not wipe a replica that is merely SLOW to sync.
#
# BUG-0046. Four facts on main, each individually reasonable, composed into a
# loop that never terminates:
#
#   1. redundancy repair respawns a non-master slot that is unreachable for
#      `confirm` ticks, and spawn_slot opens with remove_dir_all — the partial
#      transfer is DISCARDED, not resumed;
#   2. the cooldown covering that sync is hardcoded to 20 s;
#   3. re-confirmation after it costs 3 poll ticks, ~0.3 s at the defaults;
#   4. a fresh replica used to be TCP-dark for its entire full sync.
#
# So the cycle was ~20.3 s: wipe, spawn, sync, dark, cooldown, wipe again. Any
# dataset that cannot full-sync inside 20 s never finished one, and more data
# made it worse rather than better — there is no accumulation across cycles.
# The cost model's anchor node carries ~96 GB, so this was reachable by every
# managed pair at production size.
#
# Fact 4 is what #176 removed: the listener now binds BEFORE the transfer and
# `serve_loading` answers FLINTINFO with `loading:1`, so the controller's
# observe() marks the seat reachable and the repair branch never arms.
#
# WHY THIS DRILL EXISTS RATHER THAN A COMMENT. Both halves of that fix were
# already covered — loading_visible proves the SERVER answers while syncing,
# controller_managed proves the CONTROLLER manages a pair — and neither proves
# the two together, because controller_managed's syncs finish in well under a
# second. The defect lived exactly in the gap between two passing drills.
#
# The bug write-up called this "unreachable in a drill". It is reachable: cap
# the master's serve rate with FLINTCONFIG and a few MB outlast the cooldown.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-slowsync 6328 6329
fleet_guard

D=$FLINT_DRILL_ROOT/flint-slowsync
P1=6328; P2=6329
D1="$D/a"; D2="$D/b"
CLOG="$D/controller.log"
rm -rf "$D"; mkdir -p "$D"
fleet_kill controller; fleet_kill server; sleep 0.3
# KEEP THE EVIDENCE ON FAILURE. A drill that deletes the controller log it was
# asserting about costs a whole re-run to say anything, and this one is timing
# sensitive enough that the re-run may not reproduce. Same lesson as
# promote_notice keeping its seat logs.
KEEP="$FLINT_DRILL_ROOT/flint-slowsync-postmortem"
cleanup() {
  rc=$?
  if [ "$rc" != 0 ]; then
    rm -rf "$KEEP"; mkdir -p "$KEEP" 2>/dev/null
    cp "$CLOG" "$KEEP/controller.log" 2>/dev/null
    echo "  post-mortem kept in $KEEP"
  fi
  fleet_kill controller; fleet_kill server; rm -rf "$D"
}
trap cleanup EXIT

cargo build --release -q -p flint-server --features flint-server/rocks \
  || { echo "FAIL: build server"; exit 1; }
cargo build --release -q -p flint-controller || { echo "FAIL: build controller"; exit 1; }
fleet_warm ./target/release/flint-server

echo "== managed controller bootstraps the pair itself"
( ./target/release/flint-controller --manage-slots "$P1:$D1,$P2:$D2" --id SLOW \
    --poll-ms 100 --confirm 3 >"$CLOG" 2>&1 & )
fleet_wait_ping $P1
fleet_wait_ping $P2

master_port() {
  for p in $P1 $P2; do
    valkey-cli -p $p FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -q '^role:master' && { echo $p; return; }
  done
}
M=$(master_port); [ -n "$M" ] || { echo "FAIL: no master after bootstrap"; tail -20 "$CLOG"; exit 1; }
R=$([ "$M" = "$P1" ] && echo $P2 || echo $P1)
echo "  master :$M  replica :$R"

# INCOMPRESSIBLE, for the reason loading_visible spells out: repeated text
# compresses over the wire, the transfer finishes early, and the window this
# drill asserts on closes before the first sample. The failure is silent.
echo "== seed ~24 MB of incompressible values into the master"
python3 - <<'PY' | valkey-cli -p "$M" --pipe 2>&1 | tail -1
import os, sys
n, sz = 1500, 16 * 1024
out = []
for i in range(n):
    k = f"slow:{i:06d}".encode()
    v = os.urandom(sz)
    out.append(b"*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (len(k), k, len(v), v))
sys.stdout.buffer.write(b"".join(out))
PY

# 1 MiB/s against ~24 MB is ~24 s of transfer — deliberately past the 20 s
# cooldown, which is the whole condition under test. Hot-set rather than passed
# at spawn because in managed mode the CONTROLLER spawns the seats and
# spawn_slot forwards a fixed flag list.
CAP=$((1024 * 1024))
echo "== throttle the master's full-sync serve rate to $((CAP / 1024)) KiB/s"
[ "$(valkey-cli -p $M FLINTCONFIG fullsync-rate-bytes $CAP 2>&1 | tr -d '\r')" = "OK" ] \
  || { echo "FAIL: could not set fullsync-rate-bytes"; exit 1; }

RESPAWNS_BEFORE=$(grep -c 'respawning as fresh replica' "$CLOG" 2>/dev/null || true)

echo "== kill the replica: the controller must respawn it ONCE and then wait"
# ONLY the replica. fleet_kill is scoped to this drill but not to one SEAT, so
# it would take the master with it and the controller would be repairing a
# different failure than the one under test. SHUTDOWN addresses exactly one
# port, and the controller's own repair path is what must react to it.
# BY PID, because flint-server does not implement SHUTDOWN — `valkey-cli
# SHUTDOWN` against it is an error swallowed by `|| true`, i.e. a no-op that
# looks like a kill. The first version of this drill did exactly that and then
# reported "the replacement never reported loading:1", which was true and
# meant nothing: the replica had never gone down.
#
# END-ANCHORED for the usual reason: a bare "--port 6329" also matches 63290.
RPID=$(pgrep -f "flint-server --port $R( |\$)" | head -1)
[ -n "$RPID" ] || { echo "FAIL: no replica seat listening on :$R to kill"; exit 1; }
kill -9 "$RPID" 2>/dev/null
for _ in $(seq 1 50); do
  kill -0 "$RPID" 2>/dev/null || break
  sleep 0.1
done
kill -0 "$RPID" 2>/dev/null && { echo "FAIL: replica pid $RPID survived SIGKILL"; exit 1; }
[ "$(valkey-cli -p $M FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -cx 'role:master')" = 1 ] \
  || { echo "FAIL: the master went down with the replica — wrong failure under test"; exit 1; }

# POSITIVE CONTROL, and without it every assertion below is vacuous: the
# replacement must actually still be LOADING after the cooldown has expired.
# If the sync finishes inside 20 s the loop could not have armed on any build,
# and a green result would say nothing about the fix.
SAW_LOADING=0; LOADING_AT=0
T0=$(date +%s)
while [ $(( $(date +%s) - T0 )) -lt 30 ]; do
  if valkey-cli -p "$R" FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -qx 'loading:1'; then
    SAW_LOADING=1; LOADING_AT=$(( $(date +%s) - T0 ))
  fi
  sleep 1
done

[ "$SAW_LOADING" = 1 ] || {
  echo "FAIL: the replacement never reported loading:1 in 30 s — the drill never"
  echo "      created the condition it asserts on, so a pass would be vacuous."
  echo "      This is NOT evidence about the controller; it means the setup did"
  echo "      not take. The three ways that happens, in order of likelihood:"
  echo "        - the replica never actually went down, so nothing was respawned"
  echo "        - it was respawned and the sync finished inside the sample window"
  echo "        - the rate cap did not apply to the transfer"
  echo "      what :$R says now:"
  valkey-cli -p "$R" FLINTINFO 2>&1 | tr '\r' '\n' | grep -E '^(role|loading|latest_seq)' | sed 's/^/        /'
  echo "      dbsize :$R = $(valkey-cli -p "$R" DBSIZE 2>&1 | tr -d '\r')  (1500 = fully synced)"
  echo "      master :$M fullsync rate = $(valkey-cli -p "$M" FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -c 'fullsync') fields"
  echo "      respawns seen: $(grep -c 'respawning as fresh replica' "$CLOG" 2>/dev/null || echo 0)"
  tail -20 "$CLOG" | sed 's/^/    ctl| /'; exit 1; }
[ "$LOADING_AT" -ge 21 ] || {
  echo "FAIL: the replacement stopped loading after ${LOADING_AT}s, inside the 20 s"
  echo "      cooldown. The wipe loop could not arm on ANY build at this speed,"
  echo "      so this run proves nothing. Raise the dataset size or lower the cap."
  exit 1; }
echo "  still loading ${LOADING_AT}s in — past the 20 s cooldown, so the loop could arm"

RESPAWNS_AFTER=$(grep -c 'respawning as fresh replica' "$CLOG" 2>/dev/null || true)
NEW=$(( RESPAWNS_AFTER - RESPAWNS_BEFORE ))
[ "$NEW" -le 1 ] || {
  echo "FAIL: the controller respawned the syncing replica $NEW times in 30 s."
  echo "      That is BUG-0046: a seat whose sync outlasts the 20 s cooldown is"
  echo "      wiped and restarted from zero forever, making no progress."
  grep -n 'respawning as fresh replica' "$CLOG" | sed 's/^/    ctl| /'; exit 1; }

echo "  controller respawned it $NEW time(s) — it waited instead of wiping"

echo "== and the replica must actually finish, not merely be left alone"
fleet_wait_ready "$R"
valkey-cli -p "$R" FLINTINFO 2>/dev/null | tr '\r' '\n' | grep -qx 'loading:0' \
  || { echo "FAIL: replica never left the loading state"; exit 1; }
DB=$(valkey-cli -p "$R" DBSIZE 2>/dev/null | tr -d '\r')
[ "$DB" = 1500 ] || { echo "FAIL: replica converged with $DB keys, want 1500"; exit 1; }

echo "PASS: a managed controller leaves a slow-syncing replica alone (BUG-0046) —"
echo "      still loading ${LOADING_AT}s past the kill, respawned $NEW time(s), and"
echo "      converged to $DB keys"
