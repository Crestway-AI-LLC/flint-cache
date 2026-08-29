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

# --- arm D: a genuinely SUPERSEDED ex-master, with a valid candidate ---------
# Arms A and C have B catch up to A before the kill, so neither builds the
# condition BUG-0071 is about: an ex-master holding writes the survivor NEVER
# received. That is the case whose failure costs 94.2 s at
# min-replicas-to-write=1, and until now nothing exercised it.
#
# This arm is the WORKING half: divergence plus a snapshot on the correct side
# of the branch point must still rewind. The broken half -- the same setup with
# that snapshot quarantined -- is BUG-0071 and is not asserted here because it
# currently re-seeds by design.
echo "== arm D: an ex-master holding writes B never saw still rewinds, given a candidate"
fleet_kill server; sleep 0.4
rm -rf "$D/d-a" "$D/d-b" "$D/snaps-d"
$B --port 6405 --engine rocks --data-dir "$D/d-a" >"$D/d-a1.log" 2>&1 &
fleet_wait_ready 6405
$B --port 6406 --engine rocks --data-dir "$D/d-b" --replica-of 127.0.0.1:6405 >"$D/d-b1.log" 2>&1 &
fleet_wait_ready 6406
for i in $(seq 1 200); do valkey-cli -p 6405 SET "d-pre:$i" "v$i" >/dev/null; done
D_TIP=$(valkey-cli -p 6405 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
wait_seq 6406 "$D_TIP" || { echo "FAIL: arm D — B never caught up before diverging"; exit 1; }
# The candidate: taken while the two are still in step, so its seq is at or
# below what B will carry into the promotion fence.
valkey-cli -p 6405 FLINTSNAPSHOT "$D/snaps-d" >/dev/null

# DIVERGE. B stops receiving; A keeps accepting. This is the whole point of the
# arm, so it is asserted rather than assumed.
kill -9 "$(pgrep -f "flint-server --port 6406" | head -1)" 2>/dev/null
sleep 0.4
for i in $(seq 1 200); do valkey-cli -p 6405 SET "d-div:$i" "x$i" >/dev/null; done
valkey-cli -p 6405 FLINTSNAPSHOT "$D/snaps-d" >/dev/null   # a candidate PAST the fence
A_DIV=$(valkey-cli -p 6405 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
[ "${A_DIV:-0}" -gt "${D_TIP:-0}" ] || {
  echo "FAIL: arm D staged no divergence (A $A_DIV vs sync point $D_TIP) — the arm would pass vacuously"; exit 1; }
echo "  A advanced to $A_DIV past the $D_TIP B last saw"

kill -9 "$(pgrep -f "flint-server --port 6405" | head -1)" 2>/dev/null
sleep 0.4
$B --port 6406 --engine rocks --data-dir "$D/d-b" >"$D/d-b2.log" 2>&1 &
fleet_wait_ready 6406
valkey-cli -p 6406 FLINTPROMOTE 0 2 | grep -q "OK promoted" || { echo "FAIL: arm D promote"; exit 1; }
echo "drill: superseded copy rejoining" > "$D/d-a/NEEDS_RESEED"
$B --port 6405 --engine rocks --data-dir "$D/d-a" --replica-of 127.0.0.1:6406 \
   --rewind-snaps "$D/snaps-d" >"$D/d-a2.log" 2>&1 &
fleet_wait_ready 6405

grep -q "is past the fence" "$D/d-a2.log" || {
  echo "FAIL: arm D — the post-divergence snapshot was NOT refused by the fence."
  echo "      It carries writes the new master never had; accepting it is time travel."
  sed 's/^/    /' "$D/d-a2.log"; exit 1; }
grep -q "clears the fence" "$D/d-a2.log" || {
  echo "FAIL: arm D — no candidate cleared the fence, so nothing reports WHICH was chosen."
  sed 's/^/    /' "$D/d-a2.log"; exit 1; }
grep -q "rewound to" "$D/d-a2.log" || {
  echo "FAIL: arm D — a superseded ex-master with a valid candidate did not rewind."
  echo "      This is BUG-0071's 94.2 s path: the full re-seed holds the write"
  echo "      gate shut for the whole transfer at min-replicas-to-write=1."
  sed 's/^/    /' "$D/d-a2.log"; exit 1; }
grep -q "full sync: received" "$D/d-a2.log" && {
  echo "FAIL: arm D — a full checkpoint was transferred despite a valid candidate"; exit 1; }
echo "  past-the-fence snapshot refused, the older one cleared it, rewound without a transfer"

# --- arms E/F: a quarantine must not outlive the reason for it (BUG-0071) ---
# quarantine_unresumable disqualifies EVERY snapshot at or below a cursor when a
# tail hits a WAL gap. That breadth is deliberate -- narrowing it costs N failed
# boots instead of one to reach the re-seed. What was wrong is that it was also
# PERMANENT: the premise is "this master's WAL cannot reach these sequences",
# a fact about one master at one moment, and a promotion invalidates it. A
# snapshot removed for a purge stayed removed for the later fence that needed
# it, which is a 94.2 s full re-seed at min-replicas-to-write=1.
#
# The name now records the cursor, and a lower fence may reconsider it. Both
# directions are asserted: re-admitting unconditionally would reopen BUG-0062's
# livelock, so the arm that keeps it OUT matters as much as the one that lets
# it in.
diverged_rejoin() {   # $1 = tag, $2 = quarantine prefix ("" = leave as a live candidate)
  local tag=$1 qpfx=$2
  fleet_kill server; sleep 0.4
  rm -rf "$D/$tag-a" "$D/$tag-b" "$D/snaps-$tag"
  $B --port 6405 --engine rocks --data-dir "$D/$tag-a" >"$D/$tag-a1.log" 2>&1 &
  fleet_wait_ready 6405
  $B --port 6406 --engine rocks --data-dir "$D/$tag-b" --replica-of 127.0.0.1:6405 >"$D/$tag-b1.log" 2>&1 &
  fleet_wait_ready 6406
  for i in $(seq 1 200); do valkey-cli -p 6405 SET "$tag-pre:$i" "v$i" >/dev/null; done
  local tip; tip=$(valkey-cli -p 6405 FLINTINFO | tr '\r' '\n' | sed -n 's/^latest_seq://p')
  wait_seq 6406 "$tip" || { echo "FAIL: $tag — B never caught up"; exit 1; }
  valkey-cli -p 6405 FLINTSNAPSHOT "$D/snaps-$tag" >/dev/null    # the candidate
  kill -9 "$(pgrep -f "flint-server --port 6406" | head -1)" 2>/dev/null; sleep 0.4
  for i in $(seq 1 200); do valkey-cli -p 6405 SET "$tag-div:$i" "x$i" >/dev/null; done
  valkey-cli -p 6405 FLINTSNAPSHOT "$D/snaps-$tag" >/dev/null    # past the fence
  # Quarantine the GOOD candidate (the one at or below the coming fence).
  if [ -n "$qpfx" ]; then
    local f
    for f in "$D/snaps-$tag"/snap-*-seq"$tip"-*; do
      [ -e "$f" ] || continue
      mv "$f" "$(dirname "$f")/$qpfx$(basename "$f")"
    done
  fi
  kill -9 "$(pgrep -f "flint-server --port 6405" | head -1)" 2>/dev/null; sleep 0.4
  $B --port 6406 --engine rocks --data-dir "$D/$tag-b" >"$D/$tag-b2.log" 2>&1 &
  fleet_wait_ready 6406
  valkey-cli -p 6406 FLINTPROMOTE 0 2 | grep -q "OK promoted" || { echo "FAIL: $tag promote"; exit 1; }
  echo "drill: superseded copy rejoining" > "$D/$tag-a/NEEDS_RESEED"
  $B --port 6405 --engine rocks --data-dir "$D/$tag-a" --replica-of 127.0.0.1:6406 \
     --rewind-snaps "$D/snaps-$tag" >"$D/$tag-a2.log" 2>&1 &
  fleet_wait_ready 6405
}

echo "== arm E: a snapshot quarantined ABOVE the coming fence is reconsidered"
# c999999 models a purge that disqualified everything up to 999999; the
# promotion below fences far lower, so the premise no longer holds.
diverged_rejoin qe "unresumable-c999999-"
grep -q "rewound to" "$D/qe-a2.log" || {
  echo "FAIL: arm E — a quarantined snapshot was not reconsidered under a LOWER fence."
  echo "      This is BUG-0071: the re-seed holds the write gate shut for the whole"
  echo "      transfer at min-replicas-to-write=1 (94.2 s measured)."
  sed 's/^/    /' "$D/qe-a2.log"; exit 1; }
grep -q "full sync: received" "$D/qe-a2.log" && { echo "FAIL: arm E transferred a checkpoint anyway"; exit 1; }
echo "  reconsidered under a lower fence, rewound without a transfer"

echo "== arm F: quarantined AT or BELOW the fence stays out (BUG-0062 stays closed)"
# c1 is below any fence here, so the disqualifying condition still covers it.
# Re-admitting this one is what would loop: restore, refuse, restore, refuse.
diverged_rejoin qf "unresumable-c1-"
grep -q "rewound to" "$D/qf-a2.log" && {
  echo "FAIL: arm F — a snapshot still covered by its quarantine was re-admitted."
  echo "      Unconditional re-admission reopens BUG-0062's livelock."
  sed 's/^/    /' "$D/qf-a2.log"; exit 1; }
grep -q "full sync: received" "$D/qf-a2.log" || {
  echo "FAIL: arm F — neither rewound nor re-seeded; the arm proved nothing"
  sed 's/^/    /' "$D/qf-a2.log"; exit 1; }
echo "  stayed quarantined, re-seeded as before"

echo "PASS: rewind rejoin drill — ex-master rejoined from its own snapshot (no full transfer), adopted the new epoch, converged; a past-the-fence snapshot was refused client- and server-side and fell back to a re-seed that dropped the abandoned branch; and a genuinely SUPERSEDED ex-master, holding writes the survivor never received, still rewinds when a candidate on the correct side of the branch point survives (arm D); and a quarantine no longer outlives its reason -- reconsidered under a LOWER fence, still refused under one it covers (arms E/F, BUG-0071)"
