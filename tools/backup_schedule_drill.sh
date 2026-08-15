#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The backup policy loop (ADR-0011 D8): backup / verify / rehearse as
# scheduled jobs, plus retention, plus verification item 9.
#
#   0. CAPABILITY — the loop produces sets on cadence, verifies them, and a
#      REHEARSAL (a real `restore` of the newest set into scratch) succeeds
#      and stamps the alertable metric.
#   1. RETENTION — with --keep 2, older sets are pruned; the store never
#      holds more than two.
#   2. ITEM 9 — corrupt the newest set and the rehearsed metric goes STALE:
#      rehearse attempts keep counting (and failing), the backup job keeps
#      its own count, but rehearsed_at stops advancing. A policy whose
#      health is read from run counts calls this cluster healthy; the one
#      metric that tells the truth is the age of the newest artifact that
#      actually restored.
#
# (Item 8 — a run outlasting its interval never overlaps itself — is a
# flint-sched unit test, where a synthetic slow job makes it deterministic.)
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-bksched 6944
fleet_guard
B=./target/release/flint-server
BK=./target/release/flint-backup
D=$FLINT_DRILL_ROOT/flint-bksched
fleet_kill server; sleep 0.3
SCHED_PID=""
cleanup() {
  [ -n "$SCHED_PID" ] && kill -9 "$SCHED_PID" 2>/dev/null
  fleet_kill server; rm -rf "$D"
}
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

echo "== a single-pair fleet with a corpus"
$B --port 6944 --engine rocks --data-dir "$D/m" 2>"$D/m.log" &
disown
fleet_wait_listen 6944
for i in $(seq 1 100); do printf 'SET sk:%03d v%03d\r\n' "$i" "$i"; done \
  | valkey-cli -p 6944 --pipe 2>&1 | tail -1
printf 'cp\n' >"$D/cp-state"

status() { cat "$D/status" 2>/dev/null; }
field() { status | grep "^$1" | head -1 | awk -v n="$2" '{print $n}'; }

# Two planted partials — set prefixes WITHOUT a manifest, the litter of a
# run killed mid-upload. The DEAD one (older than the backup interval)
# must be swept; the FRESH one (timestamped now) must survive, because it
# could be another invocation still uploading. Neither may count against
# --keep: a prune that ranks husks would evict restorable sets while
# keeping garbage.
mkdir -p "$D/sets/backup-1000000000000/pairs/0"
printf 'half an sst' > "$D/sets/backup-1000000000000/pairs/0/000004.sst"
FRESH_ID="backup-$(($(date +%s) * 1000 + 999999))"   # timestamped in the future: never older than the interval
mkdir -p "$D/sets/$FRESH_ID/pairs/0"
printf 'still uploading' > "$D/sets/$FRESH_ID/pairs/0/000004.sst"

echo
echo "== 0/1. fast cadence with --keep 2: sets appear, verify, rehearse, prune, sweep"
$BK schedule --pairs "127.0.0.1:6944,127.0.0.1:6944" --cp-state "$D/cp-state" \
    --to "$D/sets" --snap-root "$D/snaps" \
    --every 3s --verify-every 4s --rehearse-every 4s --keep 2 \
    --status-file "$D/status" 2>"$D/sched.log" &
SCHED_PID=$!
disown

# Wait until three backups have RUN (not merely elapsed time — under load
# the cadence stretches, and a fixed sleep here would be drill #110 again).
for _ in $(seq 1 120); do
  RUNS=$(field "job backup" 4); [ "${RUNS:-0}" -ge 3 ] && break; sleep 0.5
done
[ "${RUNS:-0}" -ge 3 ] || { echo "FAIL: backup ran ${RUNS:-0} time(s)"; status; cat "$D/sched.log"; exit 1; }
[ "$(field "job backup" 6)" = "0" ] || { echo "FAIL: backup job reported failures"; status; exit 1; }

# Completed sets: at most keep=2, and every one carries its manifest —
# husks must not be counted as sets.
COMPLETED=$(ls "$D/sets" 2>/dev/null | grep '^backup-' | while read -r id; do
  [ -f "$D/sets/$id/manifest" ] && echo "$id"; done | wc -l | tr -d ' ')
[ "$COMPLETED" -le 2 ] || { echo "FAIL: $COMPLETED completed sets with --keep 2 after $RUNS backups"; exit 1; }
[ "$COMPLETED" -ge 1 ] || { echo "FAIL: no completed sets on disk at all"; exit 1; }
[ -d "$D/sets/backup-1000000000000" ] && { echo "FAIL: the DEAD partial survived the sweep"; exit 1; }
[ -d "$D/sets/$FRESH_ID" ] || { echo "FAIL: the FRESH partial was swept — a set still uploading is not garbage"; exit 1; }
echo "  $RUNS backups, $COMPLETED completed set(s) retained (keep 2); dead partial swept, fresh partial spared"

for _ in $(seq 1 60); do
  RS=$(field "rehearsed_set" 2); [ -n "$RS" ] && [ "$RS" != "none" ] && break; sleep 0.5
done
[ -n "${RS:-}" ] && [ "$RS" != "none" ] || { echo "FAIL: no rehearsal ever stamped the metric"; status; exit 1; }
VER_OK=$(field "job verify" 4)
[ "${VER_OK:-0}" -ge 1 ] || { echo "FAIL: verify job never ran"; status; exit 1; }
echo "  verify ran, rehearsed_set=$RS"
kill -9 "$SCHED_PID" 2>/dev/null; SCHED_PID=""; sleep 0.3

echo
echo "== 2. ITEM 9: a corrupt newest set stalls the REHEARSED metric, not the run counts"
rm -rf "$D/sets" "$D/status"
# Slow backups (one immediate run, then none for the drill's life), fast
# rehearsals: the newest set stays the same object throughout, so its
# corruption cannot be papered over by the next backup.
$BK schedule --pairs "127.0.0.1:6944,127.0.0.1:6944" --cp-state "$D/cp-state" \
    --to "$D/sets" --snap-root "$D/snaps" \
    --every 10m --rehearse-every 2s --keep 2 \
    --status-file "$D/status" 2>"$D/sched2.log" &
SCHED_PID=$!
disown
for _ in $(seq 1 120); do
  RS=$(field "rehearsed_set" 2); [ -n "$RS" ] && [ "$RS" != "none" ] && break; sleep 0.5
done
[ -n "${RS:-}" ] && [ "$RS" != "none" ] || { echo "FAIL: baseline rehearsal never succeeded"; status; exit 1; }
AT_BEFORE=$(field "rehearsed_at_ms" 2)
echo "  baseline: rehearsed $RS at $AT_BEFORE"

VICTIM=$(find "$D/sets/$RS/pairs" -name '*.sst' | head -1)
[ -n "$VICTIM" ] || { echo "FAIL: no SST to corrupt"; exit 1; }
python3 - "$VICTIM" <<'PY'
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read())
b[len(b)//2] ^= 0x01
open(p, 'wb').write(bytes(b))
PY
FAILS_BEFORE=$(field "job rehearse" 6)
# Wait for the rehearse job to FAIL at least twice more.
for _ in $(seq 1 120); do
  RF=$(field "job rehearse" 6)
  [ "${RF:-0}" -ge $((${FAILS_BEFORE:-0} + 2)) ] && break; sleep 0.5
done
[ "${RF:-0}" -ge $((${FAILS_BEFORE:-0} + 2)) ] || {
  echo "FAIL: rehearse never failed against the corrupt set"; status; exit 1; }
AT_AFTER=$(field "rehearsed_at_ms" 2)
[ "$AT_AFTER" = "$AT_BEFORE" ] || {
  echo "FAIL: rehearsed_at advanced ($AT_BEFORE -> $AT_AFTER) past a corrupt artifact"; exit 1; }
AGE=$(field "rehearsed_age_s" 2)
echo "  rehearse failing (failures=$RF), rehearsed_at pinned, age=${AGE}s and climbing"
status | grep '^job rehearse' | sed 's/^/  | /'

echo
echo "PASS: the policy loop backs up, verifies, rehearses and prunes — and a corrupt artifact reads as UNRESTORABLE (stale rehearsed age), never as a healthy run count"
