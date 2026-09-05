#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# How long may a replica be ABSENT before it can no longer rejoin from the
# archive, and can the node be held to that number?
#
# WHY THIS EXISTS. The Phase 1 gate condition was "the lag cap provably holds
# the WAL window", which cannot be true as written: the window in TIME is
# `archive budget / write rate`, so it FALLS as ingest rises -- precisely when
# a replica is most likely to be behind. A cap of N ms holds at one throughput
# and not at four times it. That condition was answered in the negative and
# replaced by the promise an operator actually reasons about: a replica absent
# for T seconds rejoins from the archive without a full sync, with T stated.
#
# WHICH TERM THIS EXERCISES, and why it is the TTL. Retention has two bounds
# and RocksDB applies whichever trips first -- but the size term can only trip
# when a purge pass RUNS, and that pass is throttled to
# `min(600s, ttl/2)` (BUG-0092). So the two knobs cannot be separated in a
# drill: a TTL short enough to give fast purge passes also ages files out by
# time, and a TTL long enough for the byte term to bind pushes the pass cadence
# to ten minutes. This drill therefore pins a short TTL and proves the TIME
# window directly. A size-bound window cannot be observed in under 600s, which
# is BUG-0092's subject rather than this drill's.
#
# THE TWO ARMS, and neither means anything alone:
#   INSIDE  -- absent well under the TTL: must rejoin incrementally.
#   OUTSIDE -- absent well over it: must be forced to full-sync. This is the
#              POSITIVE CONTROL. Without it, INSIDE passes on a build whose
#              archive is never pruned, on one where the replica never checks,
#              and on one where the drill's writes never reached the disk.
#              It is not hypothetical: the first version of this drill could
#              not make OUTSIDE fire, and that is how BUG-0092 was found.
#
# FLINT_WRITE_BUFFER_MB=1 is load-bearing, not tidiness: a segment is only
# ARCHIVED once its memtable has flushed, so at the default buffer a short run
# rotates no WAL and there is nothing for retention to delete -- and OUTSIDE
# would report a clean incremental rejoin because the span it meant to destroy
# was never written. That failure mode reads exactly like a pass.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-walwin 6422 6423
fleet_guard

B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-walwin; rm -rf "$D"; mkdir -p "$D"
MPORT=6422; RPORT=6423
TTL_S="${TTL_S:-8}"           # purge passes every ttl/2 = 4s
INSIDE_S="${INSIDE_S:-3}"     # comfortably under
OUTSIDE_S="${OUTSIDE_S:-24}"  # over the TTL *and* past a purge pass

fleet_kill server; sleep 0.3
# KEEP=1 leaves logs and data behind. A failing control arm is a question about
# what the archive actually did, and that cannot be answered from a directory
# the trap has already deleted.
cleanup() { fleet_kill server; [ -n "${KEEP:-}" ] || rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $*"; exit 1; }

cargo build --release -q -p flint-server --features rocks || fail "build"
fleet_warm "$B"
info() { valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr -d '\r' | sed -n "s/^$2://p"; }

# Write steadily for $1 seconds so segments rotate and then age out.
#
# 64 KiB, and the size is a Linux constraint rather than a preference: a single
# argv entry is capped at MAX_ARG_STRLEN (128 KiB, 32 pages), so the 256 KiB
# payload this started with made every valkey-cli fail with "Argument list too
# long" -- on Linux only, while macOS accepted it. The drill then wrote NOTHING,
# archived nothing, pruned nothing, and ARM 2 reported a replica rejoining
# cleanly. The control caught it, which is what it is for; the assertion below
# is so the NEXT one is named where it happens instead of three steps later.
write_for() {
  local secs="$1" payload deadline i=0 before after
  payload=$(head -c 65536 /dev/zero | tr '\0' 'x')
  before=$(info "$MPORT" latest_seq)
  deadline=$(( $(date +%s) + secs ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    valkey-cli -p "$MPORT" SET "w:$$:$i" "$payload" >/dev/null 2>&1; i=$(( i + 1 ))
  done
  after=$(info "$MPORT" latest_seq)
  # A load that did not land is not a slow load, and must not be read as one.
  if [ -z "$before" ] || [ -z "$after" ] || [ "$after" -le "$before" ]; then
    fail "wrote for ${secs}s and the master's latest_seq did not move
  (${before:-unreadable} -> ${after:-unreadable}). The writes are not reaching
  the seat, so every retention assertion after this point would be measuring
  an empty archive. Check the payload against MAX_ARG_STRLEN before anything
  else -- that is what this was the first time."
  fi
  echo "$i"
}

stop_replica() {
  fleet_signal_port $RPORT -TERM || fail "could not stop the replica on $RPORT"
  sleep 0.5
}
start_replica() {  # $1 = log name
  $B --port $RPORT --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:$MPORT \
    >"$D/$1" 2>&1 &
  fleet_wait_ready $RPORT
}
converged() {
  local target; target=$(info "$MPORT" latest_seq)
  for _ in $(seq 1 600); do
    local la; la=$(info "$RPORT" last_applied)
    [ -n "$la" ] && [ -n "$target" ] && [ "$la" -ge "$target" ] && return 0
    sleep 0.1
  done
  return 1
}
reseeded() { grep -qE "Marking for re-seed|full sync required|WalPurged" "$D/$1" 2>/dev/null; }

echo "== pair up: master $MPORT retaining ${TTL_S}s of WAL, replica $RPORT"
FLINT_WRITE_BUFFER_MB=1 $B --port $MPORT --engine rocks --data-dir "$D/m" \
  --wal-ttl-seconds "$TTL_S" --wal-size-limit-mb 1024 >"$D/m.log" 2>&1 &
fleet_wait_ready $MPORT
start_replica r1.log
N=$(write_for 2)
converged || fail "the pair never converged before the test began"
echo "   converged after $N writes; wal_bytes_per_seq=$(info $MPORT wal_bytes_per_seq)"

echo "== ARM 1 (INSIDE): replica absent ${INSIDE_S}s of a ${TTL_S}s window"
stop_replica
write_for "$INSIDE_S" >/dev/null
start_replica r2.log
converged || fail "the replica never caught up after an absence INSIDE the window"
reseeded r2.log && fail "a replica absent ${INSIDE_S}s of a ${TTL_S}s window was forced to
  full-sync. That is the promise this drill exists to hold."
echo "   rejoined incrementally, no re-seed"

echo "== ARM 2 (OUTSIDE): the positive control -- absent ${OUTSIDE_S}s"
stop_replica
write_for "$OUTSIDE_S" >/dev/null
# Started WITHOUT waiting for ready, deliberately. The whole point of this arm
# is that the tailer hits the gap, marks for re-seed and EXITS, so a seat that
# never reports ready is the expected outcome and not a bring-up failure. An
# earlier version called fleet_wait_ready here and passed on macOS only because
# the seat happened to answer PING before it read far enough to die -- timing
# luck, and it timed out at 120s the moment the write size changed.
$B --port $RPORT --engine rocks --data-dir "$D/r" --replica-of 127.0.0.1:$MPORT \
  >"$D/r3.log" 2>&1 &
RPID=$!
for _ in $(seq 1 100); do
  reseeded r3.log && break
  kill -0 "$RPID" 2>/dev/null || break   # exited: the log is final, stop polling
  sleep 0.2
done
if ! reseeded r3.log; then
  fail "a replica absent ${OUTSIDE_S}s of a ${TTL_S}s window rejoined WITHOUT a full
  sync. Either retention is not pruning (BUG-0092: the size term is only
  evaluated every 600s, so check the TTL is what is pinned here), or the load
  never reached the disk (see FLINT_WRITE_BUFFER_MB in this file's header).
  ARM 1 is not evidence of anything until this arm can fail."
fi
echo "   forced a full sync, as it must"

echo
echo "WINDOW: with --wal-ttl-seconds ${TTL_S}, a replica absent ${INSIDE_S}s rejoined from"
echo "  the archive and one absent ${OUTSIDE_S}s did not. The window is the TTL here"
echo "  because the byte budget was pinned out of the way; at the shipped defaults"
echo "  it is min(TTL, budget/rate) -- and per BUG-0092 the byte term is only"
echo "  evaluated every 600s, so the archive is unbounded between passes."
echo "PASSED"
