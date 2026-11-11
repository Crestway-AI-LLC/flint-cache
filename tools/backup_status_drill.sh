#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# ADR-0011's D8 amendment: a backup that is not working reaches someone. The
# schedule serves the age of the newest set proven restorable on a read-only
# status port, and journals a failure streak's start and end. Before it, all
# of that was in a status file only two drills read.
#
# Asserts, on real seats:
#   1. the status port answers PING and FLINTINFO, and refuses anything else
#   2. a backup that cannot reach its pair: its failures are counted, and
#      once the seat is older than the bound with nothing proven,
#      restorable_overdue is 1
#   3. the streak journals ONE BackupJobFailed, with the error, however many
#      retries fail
#   4. the pair back: the next backup proves a set, restorable_overdue is 0,
#      and ONE BackupJobRecovered is journaled
#   5. the schedule restarted: the proof is read back from its status file,
#      so the age is known before any job has run, and nothing new is
#      journaled
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-bkstat 6507 6508 6509
fleet_guard
B=./target/release/flint-server
BK=./target/release/flint-backup
CP=./target/release/flint-controlplane
D=$FLINT_DRILL_ROOT/flint-bkstat
fleet_kill server; fleet_kill controlplane; sleep 0.3
SCHED_PID=""
cleanup() {
  [ -n "$SCHED_PID" ] && kill -9 "$SCHED_PID" 2>/dev/null
  fleet_kill server; fleet_kill controlplane
  rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"
M=6507; J=6508; L=6509
body() { valkey-cli -p $L FLINTINFO 2>/dev/null | tr -d '\r'; }
field() { body | sed -n "s/^$1://p"; }
kinds() { valkey-cli -p $J CPJOURNALREAD 500 2>/dev/null | grep -c "\"kind\":\"$1\""; }
start_sched() { # start_sched <every>
  $BK schedule --pairs "127.0.0.1:$M,127.0.0.1:$M" --cp-state "$D/cp-copy" \
    --to "$D/sets" --snap-root "$D/snaps" --every "$1" --keep 2 \
    --status-file "$D/status" --listen 127.0.0.1:$L --journal 127.0.0.1:$J \
    2>>"$D/sched.log" &
  SCHED_PID=$!
  fleet_wait_listen $L
}
fail() { echo "FAIL: $*"; echo "  FLINTINFO:"; body | sed 's/^/    /'; tail -5 "$D/sched.log" | sed 's/^/  log: /'; exit 1; }

echo "== a journal, and a schedule whose pair is not up yet (every 2s: bound 5s)"
$CP --port $J --state "$D/cp-state" 2>"${FLEET_SCOPE}cp.log" &
fleet_wait_listen $J
printf 'cp\n' >"$D/cp-copy"
start_sched 2s

echo "== 1. the status port answers PING and FLINTINFO only"
[ "$(valkey-cli -p $L PING | tr -d '\r')" = PONG ] || fail "PING"
[ "$(field role)" = backup ] || fail "FLINTINFO has no role:backup"
case "$(valkey-cli -p $L SET k v 2>&1)" in
  *"read-only status port"*) ;;
  *) fail "the status port did not refuse SET: $(valkey-cli -p $L SET k v 2>&1)" ;;
esac
echo "  PONG, role:backup; SET refused"

echo "== 2. a backup that cannot reach its pair: counted, then overdue"
for _ in $(seq 1 100); do
  [ "$(field job_backup_consecutive_failures)" -ge 3 ] 2>/dev/null && [ "$(field restorable_overdue)" = 1 ] && break
  sleep 0.2
done
[ "$(field job_backup_consecutive_failures)" -ge 3 ] 2>/dev/null || fail "the failing backup was not counted"
[ "$(field restorable_overdue)" = 1 ] || fail "nothing proven past the bound, yet not overdue"
[ "$(field restorable_set)" = none ] || fail "a set reads as proven while every backup failed"
[ -n "$(field job_backup_last_error)" ] || fail "no last error for the failing job"
echo "  $(field job_backup_consecutive_failures) failures in a row; restorable_set none, overdue 1 (bound $(field restorable_bound_s)s)"

echo "== 3. the streak is journaled once, with its error"
for _ in $(seq 1 50); do [ "$(kinds BackupJobFailed)" -ge 1 ] && break; sleep 0.2; done
[ "$(kinds BackupJobFailed)" = 1 ] || fail "BackupJobFailed journaled $(kinds BackupJobFailed) times for one streak"
valkey-cli -p $J CPJOURNALREAD 500 | grep '"kind":"BackupJobFailed"' | grep -q '"cause":"[^"]' \
  || fail "BackupJobFailed carries no error"
echo "  one BackupJobFailed over $(field job_backup_failures) failed runs"

echo "== 4. the pair back: a set proven, not overdue, one BackupJobRecovered"
$B --port $M --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
fleet_wait_ping $M
[ "$(valkey-cli -p $M SET k v | tr -d '\r')" = OK ] || fail "the pair did not take a write"
for _ in $(seq 1 100); do
  [ "$(field job_backup_consecutive_failures)" = 0 ] && [ "$(field restorable_set)" != none ] && break
  sleep 0.2
done
SET=$(field restorable_set)
[ "$SET" != none ] && [ -n "$SET" ] || fail "no set proven after the pair came back"
[ -f "$D/sets/$SET/manifest" ] || fail "restorable_set $SET is not a completed set"
[ "$(field restorable_overdue)" = 0 ] || fail "overdue with a set proven $(field restorable_age_s)s ago"
for _ in $(seq 1 50); do [ "$(kinds BackupJobRecovered)" -ge 1 ] && break; sleep 0.2; done
[ "$(kinds BackupJobRecovered)" = 1 ] || fail "BackupJobRecovered journaled $(kinds BackupJobRecovered) times"
[ "$(kinds BackupJobFailed)" = 1 ] || fail "a second BackupJobFailed after recovery"
echo "  $SET proven, age $(field restorable_age_s)s, overdue 0; one BackupJobRecovered"

echo "== 5. restarted (every 10m, so no job runs): the proof is read back"
kill -9 "$SCHED_PID"; wait "$SCHED_PID" 2>/dev/null; SCHED_PID=""
start_sched 10m
B5=$(body)
RUNS=$(printf '%s\n' "$B5" | sed -n 's/^job_backup_runs://p')
GOT=$(printf '%s\n' "$B5" | sed -n 's/^restorable_set://p')
[ "$RUNS" = 0 ] || fail "a backup ran during the restart check ($RUNS), so it proves nothing"
[ -n "$GOT" ] && [ "$GOT" != none ] || fail "after a restart the proof was lost: restorable_set $GOT"
printf '%s\n' "$B5" | grep -q '^restorable_age_s:' || fail "after a restart the age is unknown"
sleep 1
[ "$(kinds BackupJobFailed)" = 1 ] && [ "$(kinds BackupJobRecovered)" = 1 ] \
  || fail "the restart journaled again ($(kinds BackupJobFailed) failed, $(kinds BackupJobRecovered) recovered)"
echo "  before any run: restorable_set $GOT, age $(printf '%s\n' "$B5" | sed -n 's/^restorable_age_s://p')s; nothing new journaled"

echo "PASS: the backup schedule serves its restorable age, overdue when nothing is proven within its bound,"
echo "      journals one row per failure streak and one per recovery, and keeps its proof across a restart."
