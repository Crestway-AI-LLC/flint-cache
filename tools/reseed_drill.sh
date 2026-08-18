#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# A replica whose cursor has fallen outside the master's retained WAL cannot
# catch up by reconnecting: every retry asks for the same purged sequence and
# gets the same answer. The old code retried anyway, once a second, forever —
# on the playground a node did that all night at live_replicas 0 while its
# pair ran unprotected, and only a manual `rm -rf` of the data dir recovered
# it. The condition arrives two ways (the master saying -WALGAP outright, or
# shipping the oldest batch it still has, which then fails apply-side), and
# BOTH were being retried.
#
# The fix is a NEEDS_RESEED marker + a non-zero exit, honoured by the next
# start. This drill proves the whole cycle, and — just as important — that an
# ordinary replica restart is untouched: silently turning every warm restart
# into a full sync would be a much worse bug than the one being fixed.
#
# It closes with the demotion half of the same contract: FLINTDEMOTE's own
# docstring has always said "wipe + resync", and now the command records that
# itself instead of trusting whichever tool restarts the seat.
#
# Requires a release build with --features rocks and valkey-cli.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
# Declared so the SET of drills can be checked for port collisions —
# fleet_init only records the scope, it changes no behaviour here. A
# drill that declares nothing is invisible to assert_no_port_overlap,
# which is how failover and controller came to share 6440/6441 and
# reseed and lag_cap to share 6471/6472, unseen.
fleet_init $FLINT_DRILL_ROOT/flint-reseed 6471 6472
BIN=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-reseed
MPORT=6471; RPORT=6472
MDIR=$D/master; RDIR=$D/replica
MLOG=$D/master.log; RLOG=$D/replica.log

pkill -9 -f "flint-server --port $MPORT" 2>/dev/null
pkill -9 -f "flint-server --port $RPORT" 2>/dev/null
sleep 0.3
cleanup() {
  pkill -9 -f "flint-server --port $MPORT" 2>/dev/null
  pkill -9 -f "flint-server --port $RPORT" 2>/dev/null
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

cargo build --release -q -p flint-server --features rocks || { echo "FAIL: build"; exit 1; }

start_master() { "$BIN" --port $MPORT --engine rocks --data-dir "$MDIR" >>"$MLOG" 2>&1 & }
# The replica runs in the foreground of its own subshell so its EXIT STATUS is
# observable: "it logged something scary" is not the assertion that matters —
# "it stopped retrying" is.
start_replica() {
  ( "$BIN" --port $RPORT --engine rocks --data-dir "$RDIR" \
      --replica-of "127.0.0.1:$MPORT" >>"$RLOG" 2>&1 ; echo $? > "$D/replica.rc" ) &
}
# $2 = tenths of a second to wait, default 100 (10s).
#
# The budget is an argument because this drill waits on two different KINDS
# of event with the same call. A process coming up is sub-second, and 10s of
# patience for it is generous. A RE-SEED is a data transfer: the marked boot
# first probes the master (internal_call_once allows itself a 5s read
# timeout), then pulls a checkpoint, then opens RocksDB, and only THEN binds
# the port (#176 — a syncing replica is invisible). Sharing one constant
# between the two meant a re-seed that was merely slow reported as "the
# replica did not come back", which reads as a crash.
wait_port() {
  for _ in $(seq 1 "${2:-100}"); do
    [ "$(valkey-cli -p "$1" PING 2>/dev/null)" = "PONG" ] && return 0
    sleep 0.1
  done
  return 1
}

# Everything known about why the replica is not answering. A bare "did not
# come back" cost a CI cycle to distinguish "still syncing" from "exited
# again", and both are visible from here.
replica_forensics() {
  echo "  still running? $(pgrep -f "flint-server --port $RPORT" >/dev/null && echo yes || echo no)"
  [ -f "$D/replica.rc" ] && echo "  exit status: $(cat "$D/replica.rc")"
  [ -f "$RDIR/NEEDS_RESEED" ] && echo "  marker still present: $(cat "$RDIR/NEEDS_RESEED")"
  echo "  replica log tail:"; tail -12 "$RLOG" 2>/dev/null | sed 's/^/    | /'
}

echo "== master + replica, in sync"
start_master; wait_port $MPORT || { echo "FAIL: master never came up"; exit 1; }
start_replica; wait_port $RPORT || { echo "FAIL: replica never came up"; exit 1; }
cli_ok valkey-cli -p $MPORT SET before v-before
for _ in $(seq 1 50); do
  [ "$(valkey-cli -p $RPORT GET before 2>/dev/null)" = "v-before" ] && break
  sleep 0.1
done
[ "$(valkey-cli -p $RPORT GET before)" = "v-before" ] \
  || { echo "FAIL: replica never caught up on a healthy link"; exit 1; }
echo "  replica has the master's writes"

echo "== a warm restart must NOT re-seed (the regression this fix could cause)"
pkill -9 -f "flint-server --port $RPORT"; sleep 0.5
: > "$RLOG"
start_replica; wait_port $RPORT || { echo "FAIL: replica did not restart"; exit 1; }
grep -q "full sync: received" "$RLOG" \
  && { echo "FAIL: an ordinary restart full-synced — every restart would now drag the whole dataset"; exit 1; }
[ "$(valkey-cli -p $RPORT GET before)" = "v-before" ] \
  || { echo "FAIL: warm restart lost data"; exit 1; }
echo "  resumed from its durable cursor, no full sync"

echo "== a MARKED boot whose cursor IS still serveable rejoins WARM"
# The POSITIVE CONTROL for the retention term added to FLINTSYNC admission
# (BUG-0015). Without this the suite only proves the probe can refuse, and a
# probe that refuses EVERYTHING would pass: every dead seat would silently pay
# a full re-seed, which is precisely what the marked warm rejoin exists to
# avoid. flintctl marks every dead seat because it cannot observe a corpse's
# role, so this — a killed replica whose copy is perfectly good — is the
# COMMON case, not an exotic one.
pkill -9 -f "flint-server --port $RPORT"; sleep 0.5
echo "killed while marked by the harness" > "$RDIR/NEEDS_RESEED"
: > "$RLOG"
start_replica; wait_port $RPORT 600 || {
  echo "FAIL: a marked replica with a serveable cursor never came back"
  replica_forensics; exit 1; }
grep -q "marked copy verified" "$RLOG" || {
  echo "FAIL: a serveable marked copy did not warm-rejoin"
  replica_forensics; exit 1; }
grep -q "full sync: received" "$RLOG" && {
  echo "FAIL: a serveable marked copy paid a full re-seed — admission is refusing what it should admit"
  replica_forensics; exit 1; }
[ -f "$RDIR/NEEDS_RESEED" ] && { echo "FAIL: the warm rejoin left the marker in place"; exit 1; }
[ "$(valkey-cli -p $RPORT GET before)" = "v-before" ] \
  || { echo "FAIL: warm-rejoined marked copy lost data"; exit 1; }
echo "  marked copy verified against the master and rejoined warm, no re-seed"

echo "== stage the purge: write past the replica, then drop the retained WAL"
CURSOR=$(valkey-cli -p $RPORT FLINTINFO 2>/dev/null | tr -d '\r' | sed -n 's/^last_applied://p')
pkill -9 -f "flint-server --port $RPORT"; sleep 0.5
awk 'BEGIN { for (i = 0; i < 20000; i++) {
  k = sprintf("gap:%06d", i); v = sprintf("v%06d", i)
  printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length(v), v } }' \
  | valkey-cli -p $MPORT --pipe >/dev/null 2>&1
# Compaction rolls the live WAL and retires the old segments into archive/,
# which is where retention expiry would eventually delete them. Deleting them
# by hand is the same end state, minutes instead of an hour.
valkey-cli -p $MPORT FLINTCOMPACT 0 >/dev/null
sleep 1
ARCHIVED=$(ls "$MDIR"/archive/*.log 2>/dev/null | wc -l | tr -d ' ')
if [ "${ARCHIVED:-0}" -eq 0 ]; then
  echo "FAIL: no archived WAL to delete — the drill never staged the condition it claims to test"
  exit 1
fi
rm -f "$MDIR"/archive/*.log
echo "  replica cursor $CURSOR; deleted $ARCHIVED archived WAL segment(s)"

echo "== the replica must diagnose it and STOP, not retry forever"
rm -f "$D/replica.rc"; : > "$RLOG"
start_replica
for _ in $(seq 1 100); do [ -f "$D/replica.rc" ] && break; sleep 0.1; done
RC=$(cat "$D/replica.rc" 2>/dev/null || echo "")
if [ -z "$RC" ]; then
  echo "FAIL: the replica is still running after 10s — this is the bug: it retries a gap that can never close"
  grep -c "reconnecting in 1s" "$RLOG" | sed 's/^/  reconnect attempts so far: /'
  exit 1
fi
[ "$RC" = "3" ] || { echo "FAIL: expected exit 3, got $RC"; tail -5 "$RLOG"; exit 1; }
grep -q "FATAL" "$RLOG" || { echo "FAIL: exited without saying why"; tail -5 "$RLOG"; exit 1; }
[ -f "$RDIR/NEEDS_RESEED" ] || { echo "FAIL: exited without marking the dir for re-seed"; exit 1; }
echo "  exit 3, marked: $(cat "$RDIR/NEEDS_RESEED")"

echo "== and the next start re-seeds itself, unattended"
: > "$RLOG"
# 60s, not 10: this wait spans a probe plus a whole checkpoint transfer.
start_replica; wait_port $RPORT 600 || {
  echo "FAIL: replica did not come back within 60s of the marked boot"
  replica_forensics; exit 1; }
grep -q "full sync: received" "$RLOG" \
  || { echo "FAIL: the marker did not trigger a full sync"; tail -5 "$RLOG"; exit 1; }
[ -f "$RDIR/NEEDS_RESEED" ] && { echo "FAIL: marker survived the re-seed; the next start would wipe again"; exit 1; }
for _ in $(seq 1 100); do
  [ "$(valkey-cli -p $RPORT GET gap:019999 2>/dev/null)" = "v019999" ] && break
  sleep 0.1
done
[ "$(valkey-cli -p $RPORT GET gap:019999)" = "v019999" ] \
  || { echo "FAIL: re-seeded replica is missing the master's data"; exit 1; }
[ "$(valkey-cli -p $RPORT GET before)" = "v-before" ] \
  || { echo "FAIL: re-seeded replica lost history the master still has"; exit 1; }
RINFO=$(valkey-cli -p $RPORT FLINTINFO | tr -d '\r')
echo "  recovered as $(echo "$RINFO" | sed -n 's/^role://p') at seq $(echo "$RINFO" | sed -n 's/^last_applied://p')"
# The measure that matters is the master's: the whole failure was a pair that
# LOOKED protected. seq_lag is master-side, so read it there.
MINFO=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r')
[ "$(echo "$MINFO" | sed -n 's/^live_replicas://p')" = "1" ] \
  || { echo "FAIL: master does not see a live replica after recovery"; exit 1; }
LAG=$(echo "$MINFO" | sed -n 's/^seq_lag://p')
[ "${LAG:-999}" -le 1 ] || { echo "FAIL: master still sees the replica behind by $LAG"; exit 1; }
echo "  master sees live_replicas 1, seq_lag $LAG"

echo "== FLINTDEMOTE records the resync contract its own docs describe"
# An ex-master's unreplicated suffix may have diverged from the successor's
# lineage, so it must never warm-rejoin. `flintctl roll-node` knew that and
# wiped; `flintctl start` did not, and nothing told the node itself.
EPOCH=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r' | sed -n 's/^role_epoch://p')
echo "  master at role epoch $EPOCH"
valkey-cli -p $MPORT FLINTDEMOTE 0 99 | sed 's/^/  /'
[ -f "$MDIR/NEEDS_RESEED" ] || { echo "FAIL: demotion did not mark the ex-master for re-seed"; exit 1; }
echo "  marked: $(cat "$MDIR/NEEDS_RESEED")"
W=$(valkey-cli -p $MPORT SET after-demote nope 2>&1)
case "$W" in *READONLY*) ;; *) echo "FAIL: demoted node still accepts writes: [$W]"; exit 1;; esac
echo "  and it is read-only"

echo "== promotion clears the marker: a promoted node IS the lineage"
# Leaving it would arm a later start-as-replica to throw away the very
# history every other node is now descended from.
valkey-cli -p $MPORT FLINTPROMOTE 0 100 | sed 's/^/  /'
[ -f "$MDIR/NEEDS_RESEED" ] && { echo "FAIL: promotion left the re-seed marker in place"; exit 1; }
[ "$(valkey-cli -p $MPORT SET after-promote yes 2>&1)" = "OK" ] \
  || { echo "FAIL: promoted node does not accept writes"; exit 1; }
[ "$(valkey-cli -p $MPORT GET before)" = "v-before" ] \
  || { echo "FAIL: promotion lost the node's data"; exit 1; }
echo "  marker gone, writes accepted, data intact"

echo "== a demoted node restarted WITHOUT --replica-of keeps its data"
# It is being started as the lineage, not as a tailer, so the marker
# describes a replication position nobody follows any more. Wiping here would
# destroy the copy everyone else descends from.
valkey-cli -p $MPORT FLINTDEMOTE 0 101 >/dev/null
[ -f "$MDIR/NEEDS_RESEED" ] || { echo "FAIL: second demotion did not mark"; exit 1; }
SEQ_BEFORE=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r' | sed -n 's/^latest_seq://p')
pkill -9 -f "flint-server --port $MPORT"; sleep 0.5
: > "$MLOG"
start_master; wait_port $MPORT || { echo "FAIL: node did not restart"; exit 1; }
# Its durable role is replica with no upstream, so keyspace reads self-fence
# (R1) — latest_seq is the honest witness that the data is still there.
SEQ_AFTER=$(valkey-cli -p $MPORT FLINTINFO | tr -d '\r' | sed -n 's/^latest_seq://p')
[ "${SEQ_AFTER:-0}" -ge "${SEQ_BEFORE:-1}" ] \
  || { echo "FAIL: a start without --replica-of discarded the data ($SEQ_BEFORE -> $SEQ_AFTER)"; exit 1; }
[ -f "$MDIR/NEEDS_RESEED" ] && { echo "FAIL: stale marker left behind; a later replica start would wipe good data"; exit 1; }
grep -q "cleared NEEDS_RESEED" "$MLOG" || { echo "FAIL: the clear was silent"; exit 1; }
echo "  data intact at seq $SEQ_AFTER, marker cleared"

echo "PASS: reseed — a cursor outside the master's retained WAL is diagnosed, marked and exited rather than retried forever; the next start re-seeds itself; demotion records the same contract; and an ordinary warm restart still resumes from its cursor"
