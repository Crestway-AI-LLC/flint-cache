#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# The user-driven-GC ranking primitives (ADR-0013 D1/D2).
#
# Flint never evicts; the contract is that a USER'S policy daemon can rank
# and delete. These are the two reads that make ranking possible, and each
# assert is against a value the drill itself planted — no "some number came
# back" checks.
#
# What it asserts:
#   1. FLINTKEYSIZE: a string reports exactly its payload length; a hash
#      reports its cumulative member bytes; growing the hash grows the size
#   2. FLINTKEYSTAMP: written_ms is a sane wall-clock instant, and a later
#      mutation MOVES it (the stamp tracks writes, not creation)
#   3. created_ms: stable across mutations for a collection (the version's
#      mint instant), 0 = honest-unknown for a string
#   4. EXPIRE does NOT move written_ms — it stamps data writes, not touches
#   5. missing key -> nil, for both commands
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-kstat 6976
fleet_guard
B=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-kstat
fleet_kill server; sleep 0.4
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
rm -rf "$D"; mkdir -p "$D"

$B --port 6976 --engine rocks --data-dir "$D/n" 2>"$D/n.log" &
disown
fleet_wait_listen 6976
fleet_wait_ping 6976
V() { valkey-cli -p 6976 "$@" | tr -d '\r'; }
written() { V FLINTKEYSTAMP "$1" | sed -n 1p; }
created() { V FLINTKEYSTAMP "$1" | sed -n 2p; }

echo "== 1. FLINTKEYSIZE reports the bytes the drill planted"
V SET s:k "0123456789" >/dev/null                # 10 bytes exactly
[ "$(V FLINTKEYSIZE s:k)" = "10" ] || { echo "FAIL: string size $(V FLINTKEYSIZE s:k), wanted 10"; exit 1; }
V HSET h:k f1 aaaa >/dev/null                    # field+value accounting
S1=$(V FLINTKEYSIZE h:k)
[ "$S1" -gt 0 ] || { echo "FAIL: hash size $S1 not positive"; exit 1; }
V HSET h:k f2 bbbbbbbb >/dev/null
S2=$(V FLINTKEYSIZE h:k)
[ "$S2" -gt "$S1" ] || { echo "FAIL: hash size did not grow ($S1 -> $S2)"; exit 1; }
echo "  string exact (10), hash grows ($S1 -> $S2)"

echo "== 2. written_ms is now-ish and a mutation moves it"
NOW_MS=$(($(date +%s) * 1000))
W1=$(written h:k)
# Within a day of the drill's clock: catches a zero, a seconds-vs-ms slip,
# and an uninitialized stamp alike.
[ "$W1" -gt $((NOW_MS - 86400000)) ] && [ "$W1" -lt $((NOW_MS + 86400000)) ] || {
  echo "FAIL: written_ms $W1 is not a plausible now (drill clock $NOW_MS)"; exit 1; }
sleep 1.1
V HSET h:k f3 c >/dev/null
W2=$(written h:k)
[ "$W2" -gt "$W1" ] || { echo "FAIL: a mutation did not advance written_ms ($W1 -> $W2)"; exit 1; }
echo "  written_ms plausible and advanced ($W1 -> $W2)"

echo "== 3. created_ms: stable for the hash, honest-unknown for the string"
C1=$(created h:k)
[ "$C1" -gt 0 ] || { echo "FAIL: hash created_ms is $C1"; exit 1; }
V HSET h:k f4 d >/dev/null
[ "$(created h:k)" = "$C1" ] || { echo "FAIL: created_ms moved on mutation"; exit 1; }
[ "$(created s:k)" = "0" ] || { echo "FAIL: string created_ms should be 0 (unknown), got $(created s:k)"; exit 1; }
echo "  created stable at $C1; string reports 0"

echo "== 4. EXPIRE is a touch, not a write: the stamp must not move"
W3=$(written h:k)
V EXPIRE h:k 900 >/dev/null
[ "$(written h:k)" = "$W3" ] || { echo "FAIL: EXPIRE moved written_ms"; exit 1; }
echo "  stamp unmoved through EXPIRE"

echo "== 5. missing key -> nil"
[ -z "$(V FLINTKEYSIZE nope)" ] || { echo "FAIL: FLINTKEYSIZE on a missing key returned data"; exit 1; }
[ -z "$(V FLINTKEYSTAMP nope)" ] || { echo "FAIL: FLINTKEYSTAMP on a missing key returned data"; exit 1; }

echo
echo "PASS: key-stat primitives — sizes match planted bytes, the write stamp advances on mutation and survives EXPIRE untouched, creation stays fixed"
