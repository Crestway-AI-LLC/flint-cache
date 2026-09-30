#!/usr/bin/env bash
# SPDX-License-Identifier: Elastic-2.0
# BUG-0194: does a rewound copy adopt the master's translated cursor when the
# translation comes back LOWER than the cursor it asked with?
#
# WHY THIS EXISTS. The 2026-09-30 rc.78 soak lost a pair on cycle 5. Both
# seats met the same thing: the new master translated the rewound copy's
# cursor into its own sequence space (`rewind attach: upstream cursor
# 28145241 (epoch (0,9)) maps to local seq 28136266`) and the copy never
# adopted it. The adoption only ever moved FORWARD, which holds when a
# master's own space runs ahead of the stream it applied -- every apply adds a
# cursor row -- and fails when the master is itself a copy that once rewound
# to an old snapshot of its own and adopted a far higher cursor. That copy's
# own space starts low and STAYS behind. On the soak the first batch then
# straddled the old-space cursor (`SequenceGap { expected: 28145242, got:
# 28145227 }`), the copy reconnected presenting the master's epoch with the
# OLD cursor, the master read that number in its own space, and a false WALGAP
# quarantined every snapshot and sent the seat to a full re-seed.
#
# THE WORSE SHAPE IS SILENT. Every batch that ends at or below the stale
# cursor is dropped as "already applied" (apply_batch's documented
# idempotence), so when the first batch past it happens to start exactly on
# it, the copy skips the whole translation offset and says nothing. This drill
# builds a window where the skipped span holds real keys, and compares every
# key and value -- a DBSIZE match alone would miss a same-count divergence.
#
# THE FIXTURE, three roles in two promotions:
#   1. A masters (0,1); B applies and runs ahead of A's space by one cursor
#      row per write. A snapshots once that lead is ~6000.
#   2. Kill A, promote B (0,2), rejoin A by rewind to that snapshot. B
#      translates UP and A adopts -- the path that has always worked -- and
#      A's own space is now B's lead BEHIND B's. It stays there: replaying
#      B's apply batches costs A one sequence per B sequence, and only B's
#      own writes (one sequence on B, an op plus a cursor row on A) close it.
#   3. B snapshots, writes on; kill B, promote A (0,3), rejoin B by rewind. A
#      translates B's cursor DOWN. That is the case under test, and the drill
#      checks the attach really went down before believing any PASS.
set -u
cd "$(dirname "$0")/.."
. "$(dirname "$0")/lib/fleet.sh"
fleet_init $FLINT_DRILL_ROOT/flint-rewind-lower 6453 6454
fleet_guard
S=./target/release/flint-server
D=$FLINT_DRILL_ROOT/flint-rewind-lower; rm -rf "$D"; mkdir -p "$D"
fleet_kill server
sleep 0.3
cleanup() { fleet_kill server; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $*"; exit 1; }

# Every key and value, as one digest. A file rather than a heredoc inside
# $( ): macOS bash 3.2 mis-parses the latter far from the cause.
cat > "$D/digest.py" <<'PY'
import hashlib, socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
f = s.makefile("rb")
def cmd(*a):
    s.sendall(("*%d\r\n" % len(a) + "".join("$%d\r\n%s\r\n" % (len(x), x) for x in a)).encode())
    return read()
def read():
    line = f.readline()
    t, rest = line[:1], line[1:-2]
    if t == b"$":
        n = int(rest)
        return None if n < 0 else f.read(n + 2)[:-2]
    if t == b"*":
        return [read() for _ in range(int(rest))]
    if t == b"-":
        raise SystemExit("error: " + rest.decode())
    return rest
keys, cur = set(), "0"
while True:
    cur, batch = cmd("SCAN", cur, "COUNT", "1000")
    keys.update(batch)
    cur = cur.decode()
    if cur == "0":
        break
keys = sorted(keys)
h = hashlib.sha256()
# GETs pipelined 500 at a time: MGET across slots is a CROSSSLOT refusal.
for i in range(0, len(keys), 500):
    chunk = keys[i:i + 500]
    s.sendall(b"".join(b"*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n" % (len(k), k) for k in chunk))
    for k in chunk:
        v = read()
        h.update(k + b"\0" + (v or b"<nil>") + b"\n")
print(len(keys), h.hexdigest())
PY

cargo build --release -q -p flint-server --features rocks || fail "build"
fleet_warm "$S"

info() {  # info <port> <field>
  valkey-cli -p "$1" FLINTINFO 2>/dev/null | tr '\r' '\n' | sed -n "s/^$2://p"
}
wait_seq() {  # wait until node $1's last_applied reaches $2 (budget: 20s)
  for _ in $(seq 1 200); do
    LA=$(info "$1" last_applied)
    [ -n "$LA" ] && [ "$LA" -ge "$2" ] && return 0
    sleep 0.1
  done
  return 1
}
writes() {  # writes <port> <prefix> <from> <to>
  for i in $(seq "$3" "$4"); do printf 'SET %s:%d v%d\n' "$2" "$i" "$i"; done \
    | valkey-cli -p "$1" >/dev/null
}
settle() {  # settle <master port> <replica port>: replica reaches the master's tip
  local tip
  tip=$(info "$1" latest_seq)
  wait_seq "$2" "$tip"
}
digest_match() {  # digest_match <what>
  local da db
  da=$(python3 "$D/digest.py" 6453) || fail "digest of A: $da"
  db=$(python3 "$D/digest.py" 6454) || fail "digest of B: $db"
  [ "$da" = "$db" ] || fail "$1: keyspaces diverge (A: $da, B: $db)"
  echo "  $1: A and B agree on every key and value ($da)"
}

echo "== 1. A(6453) masters (0,1), B(6454) applies; A snapshots once B leads by ~6000"
$S --port 6453 --engine rocks --data-dir "$D/a" >"$D/a1.log" 2>&1 &
fleet_wait_ready 6453
$S --port 6454 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6453 >"$D/b1.log" 2>&1 &
fleet_wait_ready 6454
# One write per batch on A, and one cursor row per applied batch on B: B's
# own space ends ~6000 ahead of A's. The lead AT A'S SNAPSHOT is what A
# inherits as a deficit when it rejoins from that snapshot -- a first cut
# snapshotted after 300 writes and measured A 303 sequences AHEAD.
writes 6453 pre 1 6000
settle 6453 6454 || fail "B never caught up to A before A's snapshot"
SNAP_A=$(valkey-cli -p 6453 FLINTSNAPSHOT "$D/snaps-a")
case "$SNAP_A" in OK\ snap-*-e0.1) ;; *) fail "A's snapshot id not epoch-labeled: '$SNAP_A'" ;; esac
writes 6453 pre 6001 6300
settle 6453 6454 || fail "B never caught up to A before the kill"

echo "== 2. kill A, promote B (0,2), rejoin A by rewind: the translation goes UP"
kill -9 "$(pgrep -f "flint-server --port 6453" | head -1)" 2>/dev/null
sleep 0.3
valkey-cli -p 6454 FLINTPROMOTE 0 2 | grep -q "OK promoted" || fail "promote B"
echo "drill: superseded copy rejoining" > "$D/a/NEEDS_RESEED"
$S --port 6453 --engine rocks --data-dir "$D/a" --replica-of 127.0.0.1:6454 \
   --rewind-snaps "$D/snaps-a" >"$D/a2.log" 2>&1 &
fleet_wait_ready 6453
fleet_wait_log "$D/a2.log" "rewound to" 30 \
  || { sed 's/^/    /' "$D/a2.log"; fail "A did not rewind -- stage 3 needs its own space behind B's"; }
writes 6454 p2 1 300
settle 6454 6453 || { sed 's/^/    a2: /' "$D/a2.log"; fail "A never caught up to B after its rewind"; }
digest_match "after the upward rejoin"

SNAP_B=$(valkey-cli -p 6454 FLINTSNAPSHOT "$D/snaps-b")
case "$SNAP_B" in OK\ snap-*-e0.2) ;; *) fail "B's snapshot id not epoch-labeled: '$SNAP_B'" ;; esac
# Keys B's snapshot does not hold: the rewound B must get them from A's WAL.
writes 6454 post 1 300
settle 6454 6453 || fail "A never caught up to B before B's kill"
A_OWN=$(info 6453 latest_seq); A_CUR=$(info 6453 last_applied)
case "$A_OWN$A_CUR" in ''|*[!0-9]*) fail "A's latest_seq/last_applied unreadable ('$A_OWN'/'$A_CUR')" ;; esac
[ "$A_OWN" -lt "$A_CUR" ] || fail "A's own space ($A_OWN) is not behind B's ($A_CUR): the
      next attach cannot translate downward and this drill tests nothing"
echo "  A's own space is $((A_CUR - A_OWN)) sequences behind the stream it applies"

echo "== 3. kill B, promote A (0,3), rejoin B by rewind: the translation goes DOWN"
kill -9 "$(pgrep -f "flint-server --port 6454" | head -1)" 2>/dev/null
sleep 0.3
valkey-cli -p 6453 FLINTPROMOTE 0 3 | grep -q "OK promoted" || fail "promote A"
writes 6453 p3 1 300
echo "drill: superseded copy rejoining" > "$D/b/NEEDS_RESEED"
$S --port 6454 --engine rocks --data-dir "$D/b" --replica-of 127.0.0.1:6453 \
   --rewind-snaps "$D/snaps-b" >"$D/b2.log" 2>&1 &
fleet_wait_ready 6454
fleet_wait_log "$D/b2.log" "rewound to" 30 \
  || { sed 's/^/    /' "$D/b2.log"; fail "B did not rewind -- this drill needs a rewind attach"; }
fleet_wait_log "$D/a2.log" "upstream cursor [0-9]* (epoch (0,2))" 30 \
  || { sed 's/^/    a2: /' "$D/a2.log"; fail "A logged no rewind attach for B"; }
ATTACH=$(grep -o 'rewind attach: upstream cursor [0-9]* (epoch (0,2)) maps to local seq [0-9]*' "$D/a2.log" | tail -1)
UP=$(printf '%s' "$ATTACH" | sed -n 's/.*upstream cursor \([0-9]*\) .*/\1/p')
OWN=$(printf '%s' "$ATTACH" | sed -n 's/.*local seq \([0-9]*\)$/\1/p')
[ -n "$UP" ] && [ -n "$OWN" ] || fail "A's rewind attach line is unreadable: '$ATTACH'"
[ "$OWN" -lt "$UP" ] || fail "the attach translated $UP up to $OWN -- the fixture did not
      produce the downward case"
echo "  rewind attach: B's cursor $UP -> A's own $OWN ($((UP - OWN)) lower)"

# ADOPTION FIRST, then the catch-up. Until B adopts, its last_applied is the
# old-space number, which sits ABOVE A's whole tip here -- so waiting on it
# would pass at once and compare the keyspaces before B had applied anything.
fleet_wait_log "$D/b2.log" "adopted the master's translated cursor $OWN" 30 \
  || { sed 's/^/    b2: /' "$D/b2.log"; fail "B did not adopt the translated cursor $OWN"; }
writes 6453 p3 301 600
settle 6453 6454 || { sed 's/^/    b2: /' "$D/b2.log"; fail "B never caught up to A"; }
for bad in "SequenceGap" "WALGAP" "full sync" "quarantine"; do
  grep -q "$bad" "$D/b2.log" && { sed 's/^/    b2: /' "$D/b2.log"; fail "B's rejoin logged '$bad'"; }
done
digest_match "after the downward rejoin"

echo "PASS: rewind lower space -- a copy adopts a translated cursor below its own, tails incrementally and loses nothing"
