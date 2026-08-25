#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# Does a superseded ex-master REJOIN from its own local snapshot instead of
# a full re-seed — and refuse a snapshot the fence cannot vouch for?
#
# WHY THIS EXISTS. Soak runs 26 and 27 both breached the 10s RTO budget on
# the same arithmetic: promotion + widowed grace + the killed ex-master's
# FULL re-seed, back-to-back (26231ms on run 27 was `recovered - (promote +
# grace)` to the millisecond). The re-seed grows with the dataset, so the
# published RTO silently became a function of how much data a pair holds
# (#187). The fix: FLINTPROMOTE records the branch point (promotion fence),
# masters label their scheduled snapshots with their role epoch, and a
# marked ex-master REWINDS to its newest local snapshot at or before the
# fence and tails the difference — bounded by snapshot cadence, not bytes.
#
# ARM A (the mechanism): kill a master mid-stream, promote the survivor,
# restart the ex-master marked. It must log "rewound to", must NOT transfer
# a full checkpoint, must adopt the new master's epoch, and must converge —
# including keys written only on the new timeline.
#
# ARM B (the negative control, #121's lesson): a node whose ONLY labeled
# snapshot post-dates the fence must be refused ("past the fence"), fall
# back to the full re-seed, and DROP its abandoned-branch writes. Without
# this arm the drill would keep passing if the fence check were deleted —
# the rewind would then be a machine for resurrecting dead timelines.
#
# Plus one direct protocol probe: FLINTSYNC with a cursor past the fence
# must be refused server-side even when the client-side selection is
# bypassed — the drill speaks the wire form the tailer would.
#
# Every wait here is `fleet_wait_ready`, not `fleet_wait_listen`, and that is
# load-bearing rather than tidy: this drill reads each seat's BOOT LOG for the
# decision it made — rewound, warm-rejoined, refused past the fence — and
# since #176 the listener opens BEFORE that decision is taken. Waiting on the
# socket would grep a log that is one line long ("listening ... — LOADING")
# and report the rewind as missing on a build that rewinds correctly.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rewind 6405 6406
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-rewind; rm -rf "$D"; mkdir -p "$D"
fleet_kill server
sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
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

echo "== pair up: A(6405) master, B(6406) replica"
$B --port 6405 --engine rocks --data-dir "$D/a" >"$D/a1.log" 2>&1 &
fleet_wait_ready 6405
$B --port 6406 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6405 >"$D/b1.log" 2>&1 &
fleet_wait_ready 6406

for i in $(seq 1 300); do valkey-cli -p 6405 SET "pre:$i" "v$i" >/dev/null; done
SNAP_OUT=$(valkey-cli -p 6405 FLINTSNAPSHOT "$D/snaps-a")
case "$SNAP_OUT" in OK\ snap-*-e0.1) ;; *)
  echo "FAIL: master snapshot id not epoch-labeled: '$SNAP_OUT' — without the"
  echo "      label no snapshot is ever rewind-eligible and arm A cannot pass"
  exit 1
esac
echo "  snapshot on A: $SNAP_OUT"
for i in $(seq 301 600); do valkey-cli -p 6405 SET "pre:$i" "v$i" >/dev/null; done
TIP=$(valkey-cli -p 6405 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
wait_seq 6406 "$TIP" || { echo "FAIL: B never caught up to A before the kill"; exit 1; }

echo "== arm A: kill A, promote B, rejoin A marked — it must REWIND, not re-seed"
kill -9 "$(pgrep -f "flint-server --port 6405" | head -1)" 2>/dev/null
sleep 0.3
valkey-cli -p 6406 FLINTPROMOTE 0 2 | grep -q "OK promoted" || { echo "FAIL: promote B"; exit 1; }
# LIVE LOAD through the whole rejoin. The quiet version of this drill passed
# while the loaded fleet failed in one cycle (soak run 30): under writes, the
# promoted master's own sequence numbers drift ahead of the stream positions
# the snapshot labels carry, and an untranslated attach lands off-position.
# A drill without the load never exercises the translation at all (#121).
( i=0; while :; do valkey-cli -p 6406 SET "live:$((i+=1))" "L$i" >/dev/null 2>&1; done ) &
LOADPID=$!
for i in $(seq 1 50); do valkey-cli -p 6406 SET "post:$i" "n$i" >/dev/null; done

# The mark is flintctl's job in a real fleet (start_pair_nodes writes it via
# host-mark-reseed); the drill writes the same marker to stay a raw-binary
# test of the server's contract.
echo "drill: superseded copy rejoining" > "$D/a/NEEDS_RESEED"
$B --port 6405 --engine rocks --data-dir "$D/a" --replica-of 127.0.0.1:6406 \
   --rewind-snaps "$D/snaps-a" >"$D/a2.log" 2>&1 &
fleet_wait_ready 6405

grep -q "rewound to" "$D/a2.log" || {
  echo "FAIL: rejoin did not rewind. Boot log:"; sed 's/^/    /' "$D/a2.log"; exit 1
}
grep -q "full sync: received" "$D/a2.log" && {
  echo "FAIL: a full checkpoint was transferred anyway — the rewind saved nothing"
  exit 1
}
grep -q "rewind attach: upstream cursor" "$D/b1.log" || {
  echo "FAIL: the master never translated the rewound cursor into its own"
  echo "      sequence space — an untranslated attach is off-position under"
  echo "      load (SequenceGap at best, silent replays at worst)"
  exit 1
}
# QUIESCE, THEN MEASURE. `kill` signals the load subshell and `wait` returns
# when that subshell dies — neither stops the valkey-cli it already had in
# flight. That child is reparented and its SET lands on B AFTER the tip is
# sampled: A converges to the target it was given, B holds one key more, and
# the comparison below reports "keyspaces diverge (772 vs 773) — the attach
# replayed or skipped writes" about a rejoin that did nothing wrong.
#
# Every observed failure of that assertion was off by exactly one, which is a
# straggler's signature and not a replayed span. The cost is not a flaky
# drill; it is a flaky drill that accuses the replication path of losing
# acknowledged data, which is the most expensive false positive this suite can
# produce.
#
# So wait for B's tip to STOP MOVING before treating it as a target. Same
# discipline as the converge gate, and the same shape as fleet_kill waiting
# for the process to be GONE rather than for a port to look free: never
# measure a quantity while it is still changing.
kill "$LOADPID" 2>/dev/null; wait "$LOADPID" 2>/dev/null
BTIP=""
for _ in $(seq 1 50); do
  CUR=$(valkey-cli -p 6406 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
  [ -n "$CUR" ] && [ "$CUR" = "$BTIP" ] && break
  BTIP=$CUR
  sleep 0.1
done
[ -n "$BTIP" ] || { echo "FAIL: B's tip could not be read after the load stopped"; exit 1; }
wait_seq 6405 "$BTIP" || { echo "FAIL: rewound A never converged to B's tip"; exit 1; }
DA=$(valkey-cli -p 6405 DBSIZE); DB=$(valkey-cli -p 6406 DBSIZE)
[ "$DA" = "$DB" ] || {
  echo "FAIL: keyspaces diverge after the loaded rejoin ($DA vs $DB) — the"
  echo "      attach replayed or skipped writes"
  exit 1
}
grep -q "adopted the master's role epoch (0,2)" "$D/a2.log" || {
  echo "FAIL: A never adopted B's epoch; its next reconnect would present the"
  echo "      old one and be refused at the fence its own cursor has outgrown"
  exit 1
}
[ "$(valkey-cli -p 6405 GET post:50)" = "n50" ] || { echo "FAIL: new-timeline key missing on A"; exit 1; }
[ "$(valkey-cli -p 6405 GET pre:300)" = "v300" ] || { echo "FAIL: pre-snapshot key missing on A"; exit 1; }
echo "  A rewound, adopted (0,2), converged: new-timeline and snapshot-base keys both served"

echo "== arm C: a marked REPLICA must rejoin WARM — no rewind, no re-seed"
# flintctl marks every dead seat (a corpse's role is unobservable), so the
# most common marked boot is a killed replica whose dir is a valid near-tip
# copy. Discarding or rewinding it moves the copy BACKWARD — to a last-
# mastership snapshot that ages without bound, because replicas take no
# snapshots. Soak run 34 cycle 6: a 15-minute-stale rewind target whose
# catch-up span the master's WAL had recycled, and a dead seat. The boot now
# VERIFIES the copy against the master first (the same FLINTSYNC admission a
# tailer gets) and continues from its own cursor when the lineage vouches.
kill -9 "$(pgrep -f "flint-server --port 6405" | head -1)" 2>/dev/null
sleep 0.3
for i in $(seq 51 80); do valkey-cli -p 6406 SET "post:$i" "n$i" >/dev/null; done
echo "drill: superseded copy rejoining" > "$D/a/NEEDS_RESEED"
$B --port 6405 --engine rocks --data-dir "$D/a" --replica-of 127.0.0.1:6406 \
   --rewind-snaps "$D/snaps-a" >"$D/awarm.log" 2>&1 &
fleet_wait_ready 6405
grep -q "warm rejoin at seq" "$D/awarm.log" || {
  echo "FAIL: marked replica did not rejoin warm. Boot log:"
  sed 's/^/    /' "$D/awarm.log"; exit 1
}
grep -q "rewound to" "$D/awarm.log" && {
  echo "FAIL: a valid near-tip replica was REWOUND — the copy moved backward"
  echo "      for no reason, and on a fleet the rewind target can be"
  echo "      arbitrarily stale (run 34 cycle 6)"; exit 1
}
grep -q "full sync: received" "$D/awarm.log" && {
  echo "FAIL: a valid near-tip replica was RE-SEEDED — the marker was treated"
  echo "      as a wipe order instead of a verification order"; exit 1
}
BTIP=$(valkey-cli -p 6406 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
wait_seq 6405 "$BTIP" || { echo "FAIL: warm-rejoined A never converged"; exit 1; }
[ "$(valkey-cli -p 6405 GET post:80)" = "n80" ] || { echo "FAIL: post-kill key missing after warm rejoin"; exit 1; }
echo "  A verified against B and rejoined warm from its own cursor"

echo "== protocol probe: a cursor past the fence is refused server-side"
PROBE=$(valkey-cli -p 6406 FLINTSYNC 999999999 0 1 2>&1)
echo "$PROBE" | grep -q "promotion fence" || {
  echo "FAIL: FLINTSYNC with a past-the-fence cursor answered '$PROBE' —"
  echo "      the master-side check is the backstop against a promotion racing"
  echo "      the client's own FLINTFENCE query, and it did not fire"
  exit 1
}
echo "  refused: $PROBE"

echo "== arm B: the only snapshot post-dates the fence — must re-seed, not rewind"
# A (replica) dies; B keeps writing (grace off here) and snapshots at its tip;
# then A comes back BARE and is promoted. B's snapshot now sits past the
# promotion fence: its tail is the abandoned branch.
kill -9 "$(pgrep -f "flint-server --port 6405" | head -1)" 2>/dev/null
sleep 0.3
for i in $(seq 1 200); do valkey-cli -p 6406 SET "orphan:$i" "x$i" >/dev/null; done
SNAP_B=$(valkey-cli -p 6406 FLINTSNAPSHOT "$D/snaps-b")
echo "  snapshot on B (to be orphaned): $SNAP_B"
kill -9 "$(pgrep -f "flint-server --port 6406" | head -1)" 2>/dev/null
sleep 0.3
$B --port 6405 --engine rocks --data-dir "$D/a" >"$D/a3.log" 2>&1 &
fleet_wait_ready 6405
valkey-cli -p 6405 FLINTPROMOTE 0 3 | grep -q "OK promoted" || { echo "FAIL: promote A"; exit 1; }
valkey-cli -p 6405 SET fence:probe yes >/dev/null

echo "drill: superseded copy rejoining" > "$D/b/NEEDS_RESEED"
$B --port 6406 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6405 \
   --rewind-snaps "$D/snaps-b" >"$D/b2.log" 2>&1 &
fleet_wait_ready 6406

grep -q "past the fence" "$D/b2.log" || {
  echo "FAIL: B's post-fence snapshot was not refused. Boot log:"
  sed 's/^/    /' "$D/b2.log"; exit 1
}
grep -q "full sync: received" "$D/b2.log" || {
  echo "FAIL: refusal without the re-seed fallback — B is stranded"; exit 1
}
ATIP=$(valkey-cli -p 6405 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
wait_seq 6406 "$ATIP" || { echo "FAIL: re-seeded B never converged"; exit 1; }
[ "$(valkey-cli -p 6406 GET orphan:200)" = "" ] || {
  echo "FAIL: an abandoned-branch write SURVIVED the re-seed — the fence exists"
  echo "      precisely so that timeline cannot come back"
  exit 1
}
[ "$(valkey-cli -p 6406 GET fence:probe)" = "yes" ] || { echo "FAIL: B missing the new timeline"; exit 1; }
echo "  B refused its orphaned snapshot, re-seeded, and the dead branch stayed dead"

echo "PASS: rewind rejoin drill — ex-master rejoined from its own snapshot (no full transfer), adopted the new epoch, converged; a past-the-fence snapshot was refused client- and server-side and fell back to a re-seed that dropped the abandoned branch"
