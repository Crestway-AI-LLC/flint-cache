#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Does a replica whose tail has been purged QUARANTINE the snapshots it can
# never resume from — so the next boot re-seeds instead of re-picking one and
# failing identically? (BUG-0062)
#
# WHY THIS EXISTS. On WalPurged the tailer marks the copy for re-seed and
# exits, and the next boot is supposed to full-sync. It did not: the boot's
# guard is `why.contains("promotion fence")`, a purged WAL does not say that,
# so the rewind path ran again, re-picked the SAME snapshot, re-ran the same
# probe and lost the same race. Soak 2026-08-27 cycle 2 shows one snapshot
# rewound to twice, fatal both times, and the pair left at live_replicas 0.
#
# WHY A DRILL AND NOT THE SOAK. The soak of 2026-08-27 passed all five cycles
# WITHOUT ONCE entering this path — zero WALGAP, zero quarantine. A green soak
# says the failure did not recur; it cannot say the fix works, because the race
# it depends on is narrow and may simply not fire. Hoping an hour of cloud time
# trips a TOCTOU window is not a test. This forces the condition instead, in
# seconds, deterministically, and needs no race at all: the quarantine keys off
# WalPurged in the tailer, and a stalled replica whose master recycles past it
# reaches that from the front door.
#
# THE SHAPE, and why each step is load-bearing:
#   1. A is master, snapshots (epoch-labeled — an unlabeled one is never
#      rewind-eligible and the whole drill would pass vacuously), B replicates.
#   2. Kill A, promote B. B holds tiny WAL retention, so its archive recycles.
#   3. Rejoin A marked: it REWINDS to its own snapshot while the span is still
#      retained. This is the state the bug needs — a node whose data dir came
#      from a snapshot that is about to become unreachable.
#   4. SIGSTOP A, write hard at B until B's archive rolls past A's cursor,
#      SIGCONT A. No race: the span is provably gone before A asks for it.
#   5. A's tailer must hit WalPurged, QUARANTINE, mark, and exit.
#   6. Restart A: with no candidate left it must full re-seed and converge.
#
# The controls that stop this passing for the wrong reason are asserted inline
# and named where they are.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-walgapq 6412 6413
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-walgapq; rm -rf "$D"; mkdir -p "$D"
fleet_kill server
sleep 0.3
cleanup() { kill -CONT $(pgrep -f "flint-server --port 6412") 2>/dev/null; fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }
fleet_warm "$B" ./target/release/flint-proxy ./target/release/flint-controlplane ./target/release/flint-controller

wait_seq() { # wait until node $1's last_applied reaches $2 (budget: 20s)
  for _ in $(seq 1 200); do
    LA=$(valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr '\r' '\n' | sed -n 's/^last_applied://p')
    [ -n "$LA" ] && [ "$LA" -ge "$2" ] && return 0
    sleep 0.1
  done
  return 1
}
snaps_matching() { ls "$D/snaps-a" 2>/dev/null | grep -c "^$1" | tr -d ' '; }

echo "== pair up: A(6412) master, B(6413) replica with a TINY WAL archive"
$B --port 6412 --engine rocks --data-dir "$D/a" >"$D/a1.log" 2>&1 &
fleet_wait_ready 6412
# B's retention is what recycles later, once B is the master serving A's tail.
# FLINT_WRITE_BUFFER_MB is the load-bearing half. Retention alone recycles
# nothing: a segment is only ARCHIVED once its memtable has been flushed, so
# with the default buffer the first attempt wrote 16 MB, rotated no WAL, archived
# nothing, deleted nothing, and the span stayed reachable -- the drill reported
# "neither", which is its own way of saying it tested nothing. 1 MB forces a
# flush every ~1 MB, and only then do ttl/size have anything to delete.
FLINT_WRITE_BUFFER_MB=1 $B --port 6413 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6412 \
   --wal-ttl-seconds 1 --wal-size-limit-mb 1 >"$D/b1.log" 2>&1 &
fleet_wait_ready 6413

for i in $(seq 1 300); do valkey-cli -p 6412 SET "pre:$i" "v$i" >/dev/null; done
SNAP_OUT=$(valkey-cli -p 6412 FLINTSNAPSHOT "$D/snaps-a")
case "$SNAP_OUT" in OK\ snap-*-e0.1) ;; *)
  echo "FAIL: master snapshot id not epoch-labeled: '$SNAP_OUT'"
  echo "      An unlabeled snapshot is never rewind-eligible, so every"
  echo "      assertion below would pass without the path being entered."
  exit 1
esac
echo "  snapshot on A: $SNAP_OUT"
# CONTROL: the quarantine must have something to quarantine, or step 5 proves
# nothing. Asserted before anything can remove it.
[ "$(snaps_matching snap-)" -ge 1 ] || { echo "FAIL: no snap- in snaps-a to quarantine"; exit 1; }

for i in $(seq 301 600); do valkey-cli -p 6412 SET "pre:$i" "v$i" >/dev/null; done
TIP=$(valkey-cli -p 6412 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
wait_seq 6413 "$TIP" || { echo "FAIL: B never caught up to A before the kill"; exit 1; }

echo "== kill A, promote B, rejoin A marked — it must REWIND while the span is retained"
kill -9 "$(pgrep -f "flint-server --port 6412" | head -1)" 2>/dev/null
sleep 0.3
valkey-cli -p 6413 FLINTPROMOTE 0 2 | grep -q "OK promoted" || { echo "FAIL: promote B"; exit 1; }
for i in $(seq 1 50); do valkey-cli -p 6413 SET "post:$i" "n$i" >/dev/null; done
echo "drill: superseded copy rejoining" > "$D/a/NEEDS_RESEED"
$B --port 6412 --engine rocks --data-dir "$D/a" --replica-of 127.0.0.1:6413 \
   --rewind-snaps "$D/snaps-a" >"$D/a2.log" 2>&1 &
fleet_wait_ready 6412
grep -q "rewound to" "$D/a2.log" || {
  echo "FAIL: A did not rewind, so it is not in the state the bug needs."
  sed 's/^/    /' "$D/a2.log"; exit 1
}
echo "  A rewound and is tailing"

echo "== stall A, then recycle B's archive past A's cursor"
APID=$(pgrep -f "flint-server --port 6412" | head -1)
CURSOR=$(valkey-cli -p 6412 FLINTINFO | tr '\r' '\n' | sed -n 's/^last_applied://p')
kill -STOP "$APID" || { echo "FAIL: could not stall A"; exit 1; }
# CHURN UNTIL THE PRECONDITION HOLDS, rather than guessing a volume. The
# question "is A's cursor still reachable" has an exact answer on the wire --
# it is the same FLINTSYNC admission the tailer gets -- so ask it after each
# round instead of writing a number of keys chosen by hope. The first version
# of this drill wrote a fixed 4000 keys, recycled nothing, and reported a
# failure of the FIX when what had failed was the SETUP.
V=$(head -c 4000 /dev/zero | tr '\0' 'x')
purged=0
for round in $(seq 1 12); do
  { for i in $(seq 1 2000); do echo "SET churn:$round:$i $V"; done; } \
    | valkey-cli -p 6413 >/dev/null 2>&1
  sleep 1.5   # the archive manager is TTL-driven too; 1s TTL + margin
  PROBE=$(valkey-cli -p 6413 FLINTSYNC "$CURSOR" 0 2 2>&1)
  case "$PROBE" in
    *WALGAP*) purged=1; echo "  round $round: cursor $CURSOR is now UNREACHABLE ($PROBE)"; break ;;
    *) [ "$round" = 1 ] && echo "  round $round: still reachable, churning" ;;
  esac
done
[ "$purged" = 1 ] || {
  echo "FAIL (SETUP, not the fix): after 12 rounds B still serves cursor $CURSOR."
  echo "      Last probe: $PROBE"
  echo "      Nothing below would have tested the quarantine, so this is not"
  echo "      evidence about BUG-0062 either way. Raise the rounds or shrink"
  echo "      FLINT_WRITE_BUFFER_MB / --wal-size-limit-mb on B."
  exit 1
}
# BUG-0082. THE REFUSAL MUST DESCRIBE THE ARCHIVE IT REFUSED FROM, and this is
# the one place in the suite that produces a real WALGAP, so it is the only
# place the wiring can be asserted rather than unit-tested in isolation.
#
# Why it matters here specifically: the archive is the MASTER's, and the
# replica that prints this string as its FATAL cannot stat it. If the age is
# not carried in the message it is not recoverable anywhere -- and without it,
# short retention and a cursor older than the outage produce the identical
# FATAL, which is how BUG-0082 went sixteen repairs without being diagnosed.
case "$PROBE" in
  *"archive holds"* | *"archive state could not be read"*) ;;
  *)
    echo "FAIL: the WALGAP refusal does not say what the archive held:"
    echo "      $PROBE"
    echo "      BUG-0082 needs the segment count and ages in this string. The"
    echo "      unit tests for archive_span can all pass with this call site"
    echo "      never wired up, which is exactly what this line is for."
    exit 1
    ;;
esac
echo "  the refusal describes the archive it refused from"

kill -CONT "$APID" || { echo "FAIL: could not resume A"; exit 1; }
echo "  A stalled at seq $CURSOR; B recycled past it"

echo "== A's tailer must hit WalPurged and QUARANTINE, not loop"
ok=0
for _ in $(seq 1 300); do
  grep -q "quarantine:" "$D/a2.log" && { ok=1; break; }
  sleep 0.1
done
[ "$ok" = 1 ] || {
  echo "FAIL: no quarantine after the purge. A's log:"
  tail -25 "$D/a2.log" | sed 's/^/    /'
  echo "      The precondition was PROVEN above: B answered WALGAP for this"
  echo "      cursor before A was resumed. So the span really is gone, A really"
  echo "      asked for it, and the quarantine really did not fire. This is a"
  echo "      finding about the fix, not about the drill."
  exit 1
}
grep -q "FATAL:.*never resume" "$D/a2.log" || {
  echo "FAIL: quarantined without the WalPurged escalation — wrong trigger"; exit 1
}
echo "  $(grep -c 'quarantine:' "$D/a2.log") quarantine line(s):"
grep "quarantine:" "$D/a2.log" | head -3 | sed 's/^/    /'

echo "== the snapshot is disqualified, not deleted, and records WHAT it was for"
# BUG-0071 changed this name: it now carries the cursor the snapshot was
# disqualified against (`unresumable-c<cursor>-snap-…`), because a later, LOWER
# promotion fence may legitimately reconsider it. Without the cursor there is
# nothing to reconsider, and a snapshot removed for a purge stayed removed for
# the fence that needed it -- a 94.2 s full re-seed at min-replicas-to-write=1.
[ "$(snaps_matching unresumable-c)" -ge 1 ] || {
  echo "FAIL: nothing renamed to unresumable-c<cursor>-snap-*; ls:"; ls "$D/snaps-a" | sed 's/^/    /'; exit 1
}
# The cursor in the name must be the one the quarantine reported, or a later
# fence compares against a number that means nothing.
Q_CURSOR=$(grep -oE "at or below seq [0-9]+" "$D/a2.log" | head -1 | grep -oE "[0-9]+")
[ -n "$Q_CURSOR" ] || { echo "FAIL: the quarantine did not report the cursor it used"; exit 1; }
ls "$D/snaps-a" | grep -q "^unresumable-c${Q_CURSOR}-snap-" || {
  echo "FAIL: the name does not carry the cursor the quarantine reported ($Q_CURSOR)."
  echo "      A later fence would then compare against a number that means nothing."
  ls "$D/snaps-a" | sed 's/^/    /'; exit 1
}
echo "  disqualified against cursor $Q_CURSOR, recorded in the name"
[ "$(snaps_matching snap-)" -eq 0 ] || {
  echo "FAIL: a snap- candidate survived, so the next boot can still pick one:"
  ls "$D/snaps-a" | sed 's/^/    /'; exit 1
}
echo "  snaps-a now: $(ls "$D/snaps-a" | tr '\n' ' ')"

echo "== restart A: with no candidate it must RE-SEED and converge"
for _ in $(seq 1 100); do pgrep -f "flint-server --port 6412" >/dev/null || break; sleep 0.1; done
pgrep -f "flint-server --port 6412" >/dev/null && { echo "FAIL: A did not exit after quarantining"; exit 1; }
$B --port 6412 --engine rocks --data-dir "$D/a" --replica-of 127.0.0.1:6413 \
   --rewind-snaps "$D/snaps-a" >"$D/a3.log" 2>&1 &
fleet_wait_ready 6412
grep -q "rewound to" "$D/a3.log" && {
  echo "FAIL: A rewound AGAIN — the quarantine did not remove the candidate"
  sed 's/^/    /' "$D/a3.log"; exit 1
}
TIP2=$(valkey-cli -p 6413 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
wait_seq 6412 "$TIP2" || {
  echo "FAIL: A never converged after the re-seed. Log:"; tail -20 "$D/a3.log" | sed 's/^/    /'; exit 1
}
GOT=$(valkey-cli -p 6412 GET "post:50")
[ "$GOT" = "n50" ] || { echo "FAIL: A converged but is missing new-timeline data (post:50='$GOT')"; exit 1; }

echo "PASS: a purged tail quarantines every snapshot it cannot reach, the next boot has no candidate to lose with, and the node re-seeds and converges"
