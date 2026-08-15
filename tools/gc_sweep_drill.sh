#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The GC sweeper actually runs and actually reclaims (#133).
#
# The leak this pins: the compaction filter drops expired METADATA rows,
# but a filter sees one row at a time and cannot reclaim a collection's
# subkey/zscore bodies — only gc::sweep can, and before #133 nothing
# called it. A server that never sweeps passes every functional test
# (lazy expiry hides the rows from clients), so the drill's proof is the
# sweeper's own counters, with a POSITIVE CONTROL: they must be zero
# before the corpus expires and STRICTLY CLIMB after — a sweeper that
# scans nothing "passes" trivially, and this refuses that.
#
# What it asserts:
#   0. gc-sweep-ms is a live FLINTCONFIG knob (set to 1s for the drill)
#   1. positive control — with an unexpired corpus, a full cadence
#      elapses and both counters stay 0 (the sweeper judges, not mows)
#   2. after TTLs pass: gc_swept_expired > 0 AND gc_swept_orphans > 0
#      (the hash/zset bodies went, not just their meta)
#   3. live collections survive the sweep byte-for-byte
#   4. the race shape: a key expired then RECREATED before the sweep
#      keeps its new fields — the recreate-vs-delete race from gc.rs
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-gcs 6975
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-gcs
fleet_kill server; sleep 0.4
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

$B --port 6975 --engine rocks --data-dir "$D/n" 2>"$D/n.log" &
disown
fleet_wait_listen 6975
fleet_wait_ping 6975
info() { valkey-cli -p 6975 FLINTINFO 2>/dev/null | tr -d '\r' | grep "^$1:" | cut -d: -f2; }

echo "== 0. gc-sweep-ms is live-settable"
R=$(valkey-cli -p 6975 FLINTCONFIG gc-sweep-ms 1000 | tr -d '\r')
[ "$R" = "OK" ] || { echo "FAIL: FLINTCONFIG gc-sweep-ms: $R"; exit 1; }

echo "== corpus: expiring hash+zset, live hash, and the recreate case"
cli_int valkey-cli -p 6975 HSET doomed:h f1 v1 f2 v2 f3 v3
cli_int valkey-cli -p 6975 ZADD doomed:z 1 m1 2 m2
cli_int valkey-cli -p 6975 HSET live:h keep me
cli_int valkey-cli -p 6975 HSET reborn:h old stale

echo "== 1. positive control: nothing expired yet, a cadence passes, counters stay 0"
sleep 2.5
E0=$(info gc_swept_expired); O0=$(info gc_swept_orphans)
[ -n "$E0" ] && [ -n "$O0" ] || { echo "FAIL: FLINTINFO lacks gc counters"; exit 1; }
[ "$E0" = "0" ] && [ "$O0" = "0" ] || {
  echo "FAIL: sweeper reclaimed from a fully-live corpus (expired=$E0 orphans=$O0)"; exit 1; }
echo "  counters 0/0 with everything live — the sweeper judges before it deletes"

echo "== 2. expire, then the counters must climb"
valkey-cli -p 6975 PEXPIRE doomed:h 100 >/dev/null
valkey-cli -p 6975 PEXPIRE doomed:z 100 >/dev/null
valkey-cli -p 6975 PEXPIRE reborn:h 100 >/dev/null
sleep 0.3
# The recreate-before-sweep case: reborn:h expired and comes back BEFORE
# any sweep ran — the sweep must reclaim only the old incarnation's rows.
cli_int valkey-cli -p 6975 HSET reborn:h new fresh
E1=""; O1=""
for _ in $(seq 1 20); do
  E1=$(info gc_swept_expired); O1=$(info gc_swept_orphans)
  [ "${E1:-0}" -gt 0 ] && [ "${O1:-0}" -gt 0 ] && break
  sleep 0.5
done
[ "${E1:-0}" -gt 0 ] || { echo "FAIL: gc_swept_expired never left 0 — the sweeper is not wired"; exit 1; }
[ "${O1:-0}" -gt 0 ] || { echo "FAIL: gc_swept_orphans never left 0 — bodies still leak"; exit 1; }
echo "  swept: expired=$E1 orphans=$O1"

echo "== 3. live collections survive the sweep"
[ "$(valkey-cli -p 6975 HGET live:h keep)" = "me" ] || {
  echo "FAIL: the sweep took a live hash field"; exit 1; }

echo "== 4. the recreated key kept its NEW fields (recreate-before-sweep race)"
[ "$(valkey-cli -p 6975 HGET reborn:h new)" = "fresh" ] || {
  echo "FAIL: sweep deleted a field of the recreated key"; exit 1; }
[ -z "$(valkey-cli -p 6975 HGET reborn:h old)" ] || {
  echo "FAIL: the pre-expiry field survived into the recreated key"; exit 1; }

echo
echo "PASS: gc sweeper wired and reclaiming — counters climbed from a proven zero, live and recreated keys untouched"
